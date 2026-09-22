import { randomUUID } from 'node:crypto';
import SSignal from 'ssignal';
import { JobTimeoutError, QueueClosedError } from './errors.js';
import type {
  AddJobOptions,
  CloseOptions,
  Job,
  JobContext,
  JobState,
  Queue,
  QueueError,
  QueueErrorEvent,
  QueueEventMap,
  QueueEventOptions,
  QueueJobEvent,
  QueueOptions,
  QueueSnapshot,
  QueueStats,
  QueueSubscribeOptions,
  QueueTask,
  WorkflowDefinition,
  WorkflowJobDefinition,
  WorkflowJobState,
  WorkflowState,
  WorkflowStatus,
} from './types.js';

interface ReactiveSignal<Value> {
  value: Value;
  subscribe(
    callback: (value: Value) => void,
    options?: { signal?: AbortSignal; immediate?: boolean },
  ): () => void;
}

interface ReactiveSignalConstructor {
  new <Value>(value: Value): ReactiveSignal<Value>;
}

// SSignal's published declarations use extensionless relative imports, which are valid at runtime
// but cannot be followed by typescript-eslint under NodeNext resolution. Keep the boundary typed.
const Signal = SSignal as unknown as ReactiveSignalConstructor;
const DEFAULTS = Object.freeze({
  concurrency: 1,
  restIntervalMs: 0,
  idlePollIntervalMs: 1_000,
  retentionMs: 3_600_000,
  storeResults: true,
});

interface ResolvedQueueOptions {
  concurrency: number;
  restIntervalMs: number;
  idlePollIntervalMs: number;
  retentionMs: number;
  storeResults: boolean;
}

interface InternalJob {
  id: string;
  state: JobState;
  attempt: number;
  maxAttempts: number;
  timeoutMs?: number;
  retryDelayMs?: number | ((attempt: number) => number);
  concurrencyKey?: string;
  concurrencyLimit?: number;
  key?: string;
  meta?: Readonly<Record<string, unknown>>;
  createdAt: Date;
  startedAt?: Date;
  finishedAt?: Date;
  durationMs?: number;
  cancellationRequestedAt?: Date;
  result?: unknown;
  error?: QueueError;
  task: QueueTask;
  controller: AbortController;
  retentionTimer?: NodeJS.Timeout;
  workflowId?: string;
  workflowJobName?: string;
}

interface InternalWorkflowJob {
  name: string;
  state: WorkflowJobState;
  dependsOn: readonly string[];
  definition: WorkflowJobDefinition;
  jobId?: string;
}

interface InternalWorkflow {
  id: string;
  state: WorkflowState;
  createdAt: Date;
  finishedAt?: Date;
  meta?: Readonly<Record<string, unknown>>;
  jobs: Map<string, InternalWorkflowJob>;
}

const requireInteger = (name: string, value: number, minimum: number): number => {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer greater than or equal to ${minimum}`);
  }

  return value;
};
const requireRetryDelay = (value: number | ((attempt: number) => number) | undefined): void => {
  if (value === undefined || typeof value === 'function') {
    return;
  }

  requireInteger('retryDelayMs', value, 0);
};
const resolveConcurrencyLimit = (
  concurrencyKey: string | undefined,
  concurrencyLimit: number | undefined,
): number | undefined => {
  if (concurrencyKey === undefined) {
    if (concurrencyLimit !== undefined) {
      throw new TypeError('concurrencyLimit requires concurrencyKey');
    }

    return undefined;
  }

  if (concurrencyKey.length === 0) {
    throw new TypeError('concurrencyKey cannot be empty');
  }

  return requireInteger('concurrencyLimit', concurrencyLimit ?? 1, 1);
};
const resolveOptions = (options: QueueOptions): ResolvedQueueOptions => ({
  concurrency: requireInteger('concurrency', options.concurrency ?? DEFAULTS.concurrency, 1),
  restIntervalMs: requireInteger(
    'restIntervalMs',
    options.restIntervalMs ?? DEFAULTS.restIntervalMs,
    0,
  ),
  idlePollIntervalMs: requireInteger(
    'idlePollIntervalMs',
    options.idlePollIntervalMs ?? DEFAULTS.idlePollIntervalMs,
    1,
  ),
  retentionMs: requireInteger('retentionMs', options.retentionMs ?? DEFAULTS.retentionMs, 0),
  storeResults: options.storeResults ?? DEFAULTS.storeResults,
});
const normalizeError = (reason: unknown): QueueError => {
  if (reason instanceof Error) {
    const normalized: QueueError = {
      name: reason.name,
      message: reason.message,
      ...(reason.stack === undefined ? {} : { stack: reason.stack }),
      ...(!('cause' in reason) || reason.cause === undefined ? {} : { cause: reason.cause }),
    };

    return Object.freeze(normalized);
  }

  return Object.freeze({
    name: 'Error',
    message: typeof reason === 'string' ? reason : 'Job failed with a non-Error value',
    cause: reason,
  });
};
const publicJob = (job: InternalJob): Job => {
  const snapshot: Job = {
    id: job.id,
    state: job.state,
    attempt: job.attempt,
    maxAttempts: job.maxAttempts,
    createdAt: new Date(job.createdAt),
    ...(job.key === undefined ? {} : { key: job.key }),
    ...(job.concurrencyKey === undefined ? {} : { concurrencyKey: job.concurrencyKey }),
    ...(job.meta === undefined ? {} : { meta: Object.freeze({ ...job.meta }) }),
    ...(job.startedAt === undefined ? {} : { startedAt: new Date(job.startedAt) }),
    ...(job.finishedAt === undefined ? {} : { finishedAt: new Date(job.finishedAt) }),
    ...(job.durationMs === undefined ? {} : { durationMs: job.durationMs }),
    ...(job.cancellationRequestedAt === undefined
      ? {}
      : { cancellationRequestedAt: new Date(job.cancellationRequestedAt) }),
    ...(job.result === undefined ? {} : { result: job.result }),
    ...(job.error === undefined ? {} : { error: job.error }),
  };

  return Object.freeze(snapshot);
};
const validateWorkflow = (definition: WorkflowDefinition): void => {
  const entries = Object.entries(definition.jobs);

  if (entries.length === 0) {
    throw new RangeError('workflow must contain at least one job');
  }

  const names = new Set(entries.map(([name]) => name));

  for (const [name, job] of entries) {
    if (name.length === 0) {
      throw new TypeError('workflow job names cannot be empty');
    }

    if (typeof job.run !== 'function') {
      throw new TypeError(`workflow job "${name}" must define a run function`);
    }

    requireInteger('maxAttempts', job.maxAttempts ?? 1, 1);

    if (job.timeoutMs !== undefined) {
      requireInteger('timeoutMs', job.timeoutMs, 1);
    }

    requireRetryDelay(job.retryDelayMs);
    resolveConcurrencyLimit(job.concurrencyKey, job.concurrencyLimit);

    for (const dependency of job.dependsOn ?? []) {
      if (!names.has(dependency)) {
        throw new RangeError(`workflow job "${name}" depends on unknown job "${dependency}"`);
      }

      if (dependency === name) {
        throw new RangeError(`workflow job "${name}" cannot depend on itself`);
      }
    }
  }

  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (name: string): void => {
    if (visiting.has(name)) {
      throw new RangeError('workflow dependencies must not contain a cycle');
    }

    if (visited.has(name)) {
      return;
    }

    visiting.add(name);

    for (const dependency of definition.jobs[name]?.dependsOn ?? []) {
      visit(dependency);
    }

    visiting.delete(name);
    visited.add(name);
  };

  for (const [name] of entries) {
    visit(name);
  }
};

/** Create a new in-memory processing queue. */
export const createQueue = (options: QueueOptions = {}): Queue => {
  const config = resolveOptions(options);
  const jobs = new Map<string, InternalJob>();
  const activeKeys = new Map<string, string>();
  const concurrencyCounts = new Map<string, number>();
  const pendingIds: string[] = [];
  const wakeWaiters = new Set<() => void>();
  const workflows = new Map<string, InternalWorkflow>();
  const eventListeners = new Map<
    keyof QueueEventMap,
    Set<(event: QueueEventMap[keyof QueueEventMap]) => void>
  >();

  let paused = false;
  let closed = false;
  let drainOnClose = true;
  let scheduledRetries = 0;

  const buildStats = (): QueueStats => {
    const counts: Record<JobState, number> = {
      pending: 0,
      running: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
    };

    for (const job of jobs.values()) {
      counts[job.state] += 1;
    }

    return Object.freeze({
      total: jobs.size,
      ...counts,
      paused,
      closed,
    });
  };
  const buildSnapshot = (): QueueSnapshot =>
    Object.freeze({
      jobs: Object.freeze(Array.from(jobs.values(), publicJob)),
      stats: buildStats(),
    });
  const state = new Signal<QueueSnapshot>(buildSnapshot());
  const publish = (): void => {
    state.value = buildSnapshot();
  };
  const emit = <EventName extends keyof QueueEventMap>(
    eventName: EventName,
    event: QueueEventMap[EventName],
  ): void => {
    const listeners = eventListeners.get(eventName);

    if (listeners === undefined) {
      return;
    }

    for (const listener of listeners) {
      try {
        listener(event);
      } catch {
        // Observability callbacks must not change queue execution.
      }
    }
  };
  const jobEvent = (job: InternalJob): QueueJobEvent => Object.freeze({ job: publicJob(job) });
  const errorEvent = (job: InternalJob, error: QueueError, willRetry: boolean): QueueErrorEvent =>
    Object.freeze({ job: publicJob(job), error, willRetry });
  const wake = (): void => {
    for (const resolve of wakeWaiters) {
      resolve();
    }

    wakeWaiters.clear();
  };
  const wait = (milliseconds: number, wakeable = true): Promise<void> =>
    new Promise((resolve) => {
      let settled = false;

      const finish = (): void => {
        if (settled) {
          return;
        }

        settled = true;
        clearTimeout(timer);
        wakeWaiters.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, milliseconds);

      timer.unref();

      if (wakeable) {
        wakeWaiters.add(finish);
      }
    });
  const resolveRetryDelay = (job: InternalJob): number => {
    if (job.retryDelayMs === undefined) {
      return 0;
    }

    const delay =
      typeof job.retryDelayMs === 'function' ? job.retryDelayMs(job.attempt) : job.retryDelayMs;

    return Number.isFinite(delay) && delay > 0 ? delay : 0;
  };
  const scheduleRetry = (job: InternalJob): void => {
    const delayMs = resolveRetryDelay(job);

    if (delayMs <= 0) {
      pendingIds.push(job.id);

      return;
    }

    scheduledRetries += 1;

    const timer = setTimeout(() => {
      scheduledRetries -= 1;
      pendingIds.push(job.id);
      wake();
    }, delayMs);

    timer.unref();
  };
  const adjustConcurrency = (job: InternalJob, delta: 1 | -1): void => {
    if (job.concurrencyKey === undefined) {
      return;
    }

    const next = (concurrencyCounts.get(job.concurrencyKey) ?? 0) + delta;

    if (next <= 0) {
      concurrencyCounts.delete(job.concurrencyKey);
    } else {
      concurrencyCounts.set(job.concurrencyKey, next);
    }
  };
  const enqueueJob = <Result>(
    task: QueueTask<Result>,
    addOptions: AddJobOptions = {},
    workflowLink?: { workflowId: string; workflowJobName: string },
  ): string => {
    if (closed && (workflowLink === undefined || !drainOnClose)) {
      throw new QueueClosedError();
    }

    if (typeof task !== 'function') {
      throw new TypeError('task must be a function');
    }

    const maxAttempts = requireInteger('maxAttempts', addOptions.maxAttempts ?? 1, 1);

    if (addOptions.timeoutMs !== undefined) {
      requireInteger('timeoutMs', addOptions.timeoutMs, 1);
    }

    requireRetryDelay(addOptions.retryDelayMs);

    const concurrencyLimit = resolveConcurrencyLimit(
      addOptions.concurrencyKey,
      addOptions.concurrencyLimit,
    );

    if (addOptions.key !== undefined) {
      const existing = activeKeys.get(addOptions.key);

      if (existing !== undefined) {
        return existing;
      }
    }

    const id = randomUUID();
    const job: InternalJob = {
      id,
      state: 'pending',
      attempt: 0,
      maxAttempts,
      createdAt: new Date(),
      task,
      controller: new AbortController(),
      ...(addOptions.timeoutMs === undefined ? {} : { timeoutMs: addOptions.timeoutMs }),
      ...(addOptions.retryDelayMs === undefined ? {} : { retryDelayMs: addOptions.retryDelayMs }),
      ...(addOptions.concurrencyKey === undefined
        ? {}
        : { concurrencyKey: addOptions.concurrencyKey, concurrencyLimit }),
      ...(addOptions.key === undefined ? {} : { key: addOptions.key }),
      ...(addOptions.meta === undefined ? {} : { meta: Object.freeze({ ...addOptions.meta }) }),
      ...workflowLink,
    };

    jobs.set(id, job);
    pendingIds.push(id);

    if (job.key !== undefined) {
      activeKeys.set(job.key, id);
    }

    publish();
    emit('added', jobEvent(job));

    return id;
  };
  const workflowJobState = (job: InternalWorkflowJob): WorkflowJobState =>
    job.jobId === undefined ? job.state : (jobs.get(job.jobId)?.state ?? job.state);
  const publicWorkflow = (workflow: InternalWorkflow): WorkflowStatus => {
    const workflowJobs = Object.fromEntries(
      Array.from(workflow.jobs.values(), (job) => [
        job.name,
        Object.freeze({
          name: job.name,
          state: workflowJobState(job),
          dependsOn: Object.freeze([...job.dependsOn]),
          ...(job.jobId === undefined ? {} : { jobId: job.jobId }),
        }),
      ]),
    );

    return Object.freeze({
      id: workflow.id,
      state: workflow.state,
      createdAt: new Date(workflow.createdAt),
      ...(workflow.finishedAt === undefined ? {} : { finishedAt: new Date(workflow.finishedAt) }),
      ...(workflow.meta === undefined ? {} : { meta: Object.freeze({ ...workflow.meta }) }),
      jobs: Object.freeze(workflowJobs),
    });
  };
  const advanceWorkflow = (workflow: InternalWorkflow): void => {
    let changed = true;

    while (changed) {
      changed = false;

      for (const workflowJob of workflow.jobs.values()) {
        if (workflowJob.state !== 'blocked') {
          continue;
        }

        const dependencyStates = workflowJob.dependsOn.map((dependency) =>
          workflowJobState(workflow.jobs.get(dependency) as InternalWorkflowJob),
        );

        if (
          dependencyStates.some((stateName) =>
            ['failed', 'cancelled', 'skipped'].includes(stateName),
          )
        ) {
          workflowJob.state = 'skipped';
          changed = true;
        } else if (dependencyStates.every((stateName) => stateName === 'completed')) {
          workflowJob.state = 'pending';
          workflowJob.jobId = enqueueJob(
            workflowJob.definition.run,
            {
              maxAttempts: workflowJob.definition.maxAttempts,
              timeoutMs: workflowJob.definition.timeoutMs,
              retryDelayMs: workflowJob.definition.retryDelayMs,
              concurrencyKey: workflowJob.definition.concurrencyKey,
              concurrencyLimit: workflowJob.definition.concurrencyLimit,
              meta: workflowJob.definition.meta,
            },
            { workflowId: workflow.id, workflowJobName: workflowJob.name },
          );
          changed = true;
        }
      }
    }

    const states = Array.from(workflow.jobs.values(), workflowJobState);
    const terminal = states.every((stateName) =>
      ['completed', 'failed', 'cancelled', 'skipped'].includes(stateName),
    );

    if (!terminal) {
      workflow.state = states.some((stateName) => stateName !== 'blocked') ? 'running' : 'pending';

      return;
    }

    workflow.state = states.includes('failed')
      ? 'failed'
      : states.includes('cancelled')
        ? 'cancelled'
        : 'completed';
    workflow.finishedAt = new Date();
  };
  const settleWorkflowJob = (job: InternalJob, stateName: JobState): void => {
    if (job.workflowId === undefined || job.workflowJobName === undefined) {
      return;
    }

    const workflow = workflows.get(job.workflowId);
    const workflowJob = workflow?.jobs.get(job.workflowJobName);

    if (workflow === undefined || workflowJob === undefined) {
      return;
    }

    workflowJob.state = stateName;
    advanceWorkflow(workflow);
  };
  const removeRetainedJob = (job: InternalJob): void => {
    if (jobs.delete(job.id)) {
      publish();
    }
  };
  const scheduleRetention = (job: InternalJob): void => {
    if (config.retentionMs === 0) {
      removeRetainedJob(job);

      return;
    }

    job.retentionTimer = setTimeout(() => removeRetainedJob(job), config.retentionMs);
    job.retentionTimer.unref();
  };
  const finishJob = (job: InternalJob, stateName: JobState): void => {
    job.state = stateName;
    job.finishedAt = new Date();
    job.durationMs =
      job.startedAt === undefined ? 0 : job.finishedAt.getTime() - job.startedAt.getTime();

    if (job.key !== undefined && activeKeys.get(job.key) === job.id) {
      activeKeys.delete(job.key);
    }

    publish();
    scheduleRetention(job);
    settleWorkflowJob(job, stateName);

    if (stateName === 'completed') {
      emit('completed', jobEvent(job));
    } else if (stateName === 'failed' && job.error !== undefined) {
      const event = errorEvent(job, job.error, false);

      emit('error', event);
      emit('failed', event);
    } else if (stateName === 'cancelled') {
      emit('cancelled', jobEvent(job));
    }
  };
  const runJob = async (job: InternalJob): Promise<void> => {
    job.state = 'running';
    job.attempt += 1;
    job.startedAt = new Date();
    adjustConcurrency(job, 1);
    publish();
    emit('started', jobEvent(job));

    const context: JobContext = Object.freeze({
      signal: job.controller.signal,
      jobId: job.id,
      attempt: job.attempt,
      ...(job.key === undefined ? {} : { key: job.key }),
      ...(job.meta === undefined ? {} : { meta: job.meta }),
    });

    let timeoutTimer: NodeJS.Timeout | undefined;

    try {
      const taskPromise = job.task(context);
      const result = await (job.timeoutMs === undefined
        ? taskPromise
        : Promise.race([
            taskPromise,
            new Promise<never>((_resolve, reject) => {
              timeoutTimer = setTimeout(() => {
                // Reject before aborting: a task's own abort listener may settle
                // synchronously, and rejecting first guarantees the timeout wins the race.
                reject(new JobTimeoutError(job.timeoutMs as number));
                job.controller.abort();
              }, job.timeoutMs);
              timeoutTimer.unref();
            }),
          ]));

      if (job.cancellationRequestedAt !== undefined) {
        finishJob(job, 'cancelled');
      } else {
        if (config.storeResults) {
          job.result = result;
        }

        finishJob(job, 'completed');
      }
    } catch (reason: unknown) {
      if (job.cancellationRequestedAt !== undefined) {
        finishJob(job, 'cancelled');
      } else if (job.attempt < job.maxAttempts) {
        const error = normalizeError(reason);

        job.state = 'pending';
        job.startedAt = undefined;
        publish();

        const event = errorEvent(job, error, true);

        emit('error', event);
        emit('retrying', event);
        scheduleRetry(job);
      } else {
        job.error = normalizeError(reason);
        finishJob(job, 'failed');
      }
    } finally {
      if (timeoutTimer !== undefined) {
        clearTimeout(timeoutTimer);
      }

      adjustConcurrency(job, -1);
    }
  };
  const nextPendingJob = (): InternalJob | undefined => {
    for (let index = 0; index < pendingIds.length; index += 1) {
      const id = pendingIds[index];
      const job = id === undefined ? undefined : jobs.get(id);

      if (job === undefined || job.state !== 'pending') {
        pendingIds.splice(index, 1);
        index -= 1;

        continue;
      }

      if (
        job.concurrencyKey !== undefined &&
        (concurrencyCounts.get(job.concurrencyKey) ?? 0) >= (job.concurrencyLimit ?? 1)
      ) {
        continue;
      }

      pendingIds.splice(index, 1);

      return job;
    }

    return undefined;
  };
  const worker = async (): Promise<void> => {
    while (true) {
      if (closed && (!drainOnClose || (pendingIds.length === 0 && scheduledRetries === 0))) {
        return;
      }

      if (paused && !closed) {
        await wait(config.idlePollIntervalMs);
        continue;
      }

      const job = nextPendingJob();

      if (job === undefined) {
        await wait(config.idlePollIntervalMs);
        continue;
      }

      await runJob(job);

      if (config.restIntervalMs > 0) {
        await wait(config.restIntervalMs, false);
      }
    }
  };
  const workers = Array.from({ length: config.concurrency }, worker);
  const queue: Queue = {
    add<Result>(task: QueueTask<Result>, addOptions: AddJobOptions = {}): string {
      return enqueueJob(task, addOptions);
    },

    addWorkflow(definition: WorkflowDefinition): string {
      if (closed) {
        throw new QueueClosedError();
      }

      validateWorkflow(definition);

      const id = randomUUID();
      const workflow: InternalWorkflow = {
        id,
        state: 'pending',
        createdAt: new Date(),
        ...(definition.meta === undefined ? {} : { meta: Object.freeze({ ...definition.meta }) }),
        jobs: new Map(
          Object.entries(definition.jobs).map(([name, job]) => [
            name,
            {
              name,
              state: 'blocked',
              dependsOn: Object.freeze([...(job.dependsOn ?? [])]),
              definition: job,
            },
          ]),
        ),
      };

      workflows.set(id, workflow);
      advanceWorkflow(workflow);

      return id;
    },

    getStatus<Result = unknown>(id: string): Job<Result> | undefined {
      const job = jobs.get(id);

      return job === undefined ? undefined : (publicJob(job) as Job<Result>);
    },

    getWorkflowStatus(id: string): WorkflowStatus | undefined {
      const workflow = workflows.get(id);

      return workflow === undefined ? undefined : publicWorkflow(workflow);
    },

    on<EventName extends keyof QueueEventMap>(
      eventName: EventName,
      listener: (event: QueueEventMap[EventName]) => void,
      options?: QueueEventOptions,
    ): () => void {
      if (options?.signal?.aborted === true) {
        return () => undefined;
      }

      const listeners = eventListeners.get(eventName) ?? new Set();
      const eventListener = listener as (event: QueueEventMap[keyof QueueEventMap]) => void;
      const unsubscribe = (): void => {
        listeners.delete(eventListener);
        options?.signal?.removeEventListener('abort', unsubscribe);
      };

      listeners.add(eventListener);
      eventListeners.set(eventName, listeners);
      options?.signal?.addEventListener('abort', unsubscribe, { once: true });

      return unsubscribe;
    },

    cancel(id: string): boolean {
      const job = jobs.get(id);

      if (job === undefined || !['pending', 'running'].includes(job.state)) {
        return false;
      }

      job.cancellationRequestedAt = new Date();
      job.controller.abort();

      if (job.state === 'pending') {
        finishJob(job, 'cancelled');
      } else {
        publish();
      }

      return true;
    },

    pause(): void {
      if (closed || paused) {
        return;
      }

      paused = true;
      publish();
    },

    resume(): void {
      if (closed || !paused) {
        return;
      }

      paused = false;
      publish();
      wake();
    },

    getStats(): QueueStats {
      return buildStats();
    },

    subscribe(
      listener: (snapshot: QueueSnapshot) => void,
      subscribeOptions?: QueueSubscribeOptions,
    ): () => void {
      return state.subscribe(listener, subscribeOptions);
    },

    async close(closeOptions: CloseOptions = {}): Promise<void> {
      if (!closed) {
        closed = true;
        paused = false;
        drainOnClose = closeOptions.drain ?? true;

        if (!drainOnClose) {
          for (const job of jobs.values()) {
            if (job.state === 'pending' || job.state === 'running') {
              queue.cancel(job.id);
            }
          }
        }

        publish();
        wake();
      }

      await Promise.all(workers);
    },
  };

  return Object.freeze(queue);
};
