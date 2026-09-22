/** The lifecycle states exposed for a queued job. */
export type JobState = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

/** A serializable representation of an error thrown by a job. */
export interface QueueError {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
  readonly cause?: unknown;
}

/** Context supplied to every task when it starts. */
export interface JobContext {
  readonly signal: AbortSignal;
  readonly jobId: string;
  readonly attempt: number;
  readonly key?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
}

/** A synchronous or asynchronous unit of work. */
export type QueueTask<Result = unknown> = (context: JobContext) => Result | PromiseLike<Result>;

/** Options attached to one queued task. */
export interface AddJobOptions {
  readonly key?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly maxAttempts?: number;
  /** Aborts and fails an attempt that runs longer than this, in milliseconds. */
  readonly timeoutMs?: number;
  /** Delay before a failed attempt is re-queued, in milliseconds, or a function of the failed attempt number. */
  readonly retryDelayMs?: number | ((attempt: number) => number);
  /** Jobs sharing the same `concurrencyKey` are throttled to `concurrencyLimit` running at once. */
  readonly concurrencyKey?: string;
  /** Maximum jobs sharing `concurrencyKey` that may run at once. Requires `concurrencyKey`; defaults to 1. */
  readonly concurrencyLimit?: number;
}

/** Public, immutable view of a job. */
export interface Job<Result = unknown> {
  readonly id: string;
  readonly state: JobState;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly key?: string;
  readonly concurrencyKey?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly createdAt: Date;
  readonly startedAt?: Date;
  readonly finishedAt?: Date;
  readonly durationMs?: number;
  readonly cancellationRequestedAt?: Date;
  readonly result?: Result;
  readonly error?: QueueError;
}

/** Alias used by status lookup APIs. */
export type JobStatus<Result = unknown> = Job<Result>;

/** Aggregate state for the retained jobs in a queue. */
export interface QueueStats {
  readonly total: number;
  readonly pending: number;
  readonly running: number;
  readonly completed: number;
  readonly failed: number;
  readonly cancelled: number;
  readonly paused: boolean;
  readonly closed: boolean;
}

/** Reactive snapshot emitted whenever observable queue state changes. */
export interface QueueSnapshot {
  readonly jobs: readonly Job[];
  readonly stats: QueueStats;
}

/** Queue construction options. */
export interface QueueOptions {
  readonly concurrency?: number;
  readonly restIntervalMs?: number;
  readonly idlePollIntervalMs?: number;
  readonly retentionMs?: number;
  readonly storeResults?: boolean;
}

/** Subscription options compatible with the platform AbortSignal. */
export interface QueueSubscribeOptions {
  readonly signal?: AbortSignal;
  readonly immediate?: boolean;
}

/** Payload emitted for a job lifecycle event. */
export interface QueueJobEvent {
  readonly job: Job;
}

/** Payload emitted whenever a task attempt throws. */
export interface QueueErrorEvent extends QueueJobEvent {
  readonly error: QueueError;
  readonly willRetry: boolean;
}

/** Strongly typed event names and their payloads. */
export interface QueueEventMap {
  readonly added: QueueJobEvent;
  readonly started: QueueJobEvent;
  readonly retrying: QueueErrorEvent;
  readonly completed: QueueJobEvent;
  readonly failed: QueueErrorEvent;
  readonly cancelled: QueueJobEvent;
  readonly error: QueueErrorEvent;
}

/** Event-listener options compatible with the platform AbortSignal. */
export interface QueueEventOptions {
  readonly signal?: AbortSignal;
}

/** Options controlling graceful queue shutdown. */
export interface CloseOptions {
  readonly drain?: boolean;
}

/** The lifecycle states exposed for a workflow. */
export type WorkflowState = 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

/** The state of one named job within a workflow. */
export type WorkflowJobState = 'blocked' | 'pending' | JobState | 'skipped';

/** One named unit of work and the jobs that must complete before it can run. */
export interface WorkflowJobDefinition<Result = unknown> {
  readonly run: QueueTask<Result>;
  readonly dependsOn?: readonly string[];
  readonly maxAttempts?: number;
  readonly timeoutMs?: number;
  readonly retryDelayMs?: number | ((attempt: number) => number);
  readonly concurrencyKey?: string;
  readonly concurrencyLimit?: number;
  readonly meta?: Readonly<Record<string, unknown>>;
}

/** A directed acyclic graph of named jobs. */
export interface WorkflowDefinition {
  readonly jobs: Readonly<Record<string, WorkflowJobDefinition>>;
  readonly meta?: Readonly<Record<string, unknown>>;
}

/** Public state for one named job in a workflow. */
export interface WorkflowJobStatus {
  readonly name: string;
  readonly state: WorkflowJobState;
  readonly dependsOn: readonly string[];
  readonly jobId?: string;
}

/** Public, immutable view of a workflow and its jobs. */
export interface WorkflowStatus {
  readonly id: string;
  readonly state: WorkflowState;
  readonly createdAt: Date;
  readonly finishedAt?: Date;
  readonly meta?: Readonly<Record<string, unknown>>;
  readonly jobs: Readonly<Record<string, WorkflowJobStatus>>;
}

/** Framework-neutral queue contract. */
export interface Queue {
  add<Result>(task: QueueTask<Result>, options?: AddJobOptions): string;
  addWorkflow(definition: WorkflowDefinition): string;
  getStatus<Result = unknown>(id: string): JobStatus<Result> | undefined;
  getWorkflowStatus(id: string): WorkflowStatus | undefined;
  on<EventName extends keyof QueueEventMap>(
    eventName: EventName,
    listener: (event: QueueEventMap[EventName]) => void,
    options?: QueueEventOptions,
  ): () => void;
  cancel(id: string): boolean;
  pause(): void;
  resume(): void;
  getStats(): QueueStats;
  subscribe(
    listener: (snapshot: QueueSnapshot) => void,
    options?: QueueSubscribeOptions,
  ): () => void;
  close(options?: CloseOptions): Promise<void>;
}
