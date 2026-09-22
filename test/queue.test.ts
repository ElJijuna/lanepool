import { createQueue, QueueClosedError } from '../src/index.js';

const waitFor = async (predicate: () => boolean, timeoutMs = 1_000): Promise<void> => {
  const startedAt = Date.now();

  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }

    await new Promise((resolve) => setTimeout(resolve, 2));
  }
};

describe('createQueue', () => {
  test('runs synchronous and asynchronous jobs and records results', async () => {
    const queue = createQueue({ concurrency: 2, idlePollIntervalMs: 1 });
    const syncId = queue.add(() => 42);
    const asyncId = queue.add(() => Promise.resolve('done'));

    await waitFor(() => queue.getStats().completed === 2);

    expect(queue.getStatus(syncId)).toMatchObject({ state: 'completed', result: 42 });
    expect(queue.getStatus(asyncId)).toMatchObject({ state: 'completed', result: 'done' });
    await queue.close();
  });

  test('normalizes synchronous and asynchronous failures', async () => {
    const queue = createQueue({ concurrency: 2, idlePollIntervalMs: 1 });
    const syncId = queue.add(() => {
      throw new TypeError('sync failure');
    });
    const asyncId = queue.add(async () => Promise.reject(new Error('async failure')));

    await waitFor(() => queue.getStats().failed === 2);

    expect(queue.getStatus(syncId)?.error).toMatchObject({
      name: 'TypeError',
      message: 'sync failure',
    });
    expect(queue.getStatus(asyncId)?.error).toMatchObject({
      name: 'Error',
      message: 'async failure',
    });
    await queue.close();
  });

  test('retries failed jobs at the end of the queue', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });
    const executionOrder: string[] = [];
    const retriedId = queue.add(
      ({ attempt }) => {
        executionOrder.push(`retried:${attempt}`);

        if (attempt === 1) {
          throw new Error('temporary failure');
        }

        return 'inserted';
      },
      { maxAttempts: 2 },
    );

    queue.add(() => {
      executionOrder.push('next');
    });

    await waitFor(() => queue.getStats().completed === 2);

    expect(executionOrder).toEqual(['retried:1', 'next', 'retried:2']);
    expect(queue.getStatus(retriedId)).toMatchObject({
      state: 'completed',
      attempt: 2,
      maxAttempts: 2,
      result: 'inserted',
    });
    await queue.close();
  });

  test('records the final failure after exhausting attempts', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const id = queue.add(
      ({ attempt }) => {
        throw new Error(`failure ${attempt}`);
      },
      { maxAttempts: 2 },
    );

    await waitFor(() => queue.getStatus(id)?.state === 'failed');

    expect(queue.getStatus(id)).toMatchObject({
      state: 'failed',
      attempt: 2,
      maxAttempts: 2,
      error: { message: 'failure 2' },
    });
    await queue.close();
  });

  test('aborts and fails an attempt that exceeds timeoutMs', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });

    let observedAbort = false;

    const id = queue.add(
      ({ signal }) =>
        new Promise<void>((resolve) => {
          signal.addEventListener(
            'abort',
            () => {
              observedAbort = true;
              resolve();
            },
            { once: true },
          );
        }),
      { timeoutMs: 5 },
    );

    await waitFor(() => queue.getStatus(id)?.state === 'failed');

    expect(observedAbort).toBe(true);
    expect(queue.getStatus(id)?.error).toMatchObject({ name: 'JobTimeoutError' });
    await queue.close();
  });

  test('retries a timed-out attempt when attempts remain', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const attempts: number[] = [];
    const id = queue.add(
      ({ attempt, signal }) => {
        attempts.push(attempt);

        if (attempt === 1) {
          return new Promise<string>(() => {
            signal.addEventListener('abort', () => undefined, { once: true });
          });
        }

        return 'recovered';
      },
      { maxAttempts: 2, timeoutMs: 5 },
    );

    await waitFor(() => queue.getStatus(id)?.state === 'completed');

    expect(attempts).toEqual([1, 2]);
    expect(queue.getStatus(id)).toMatchObject({ state: 'completed', result: 'recovered' });
    await queue.close();
  });

  test('emits typed lifecycle and error events for every attempt', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const lifecycle: string[] = [];
    const errors: Array<{ message: string; willRetry: boolean }> = [];

    for (const eventName of ['added', 'started', 'retrying', 'completed', 'failed'] as const) {
      queue.on(eventName, () => lifecycle.push(eventName));
    }

    queue.on('error', ({ error, willRetry }) => {
      errors.push({ message: error.message, willRetry });
    });

    const retriedId = queue.add(
      ({ attempt }) => {
        if (attempt === 1) {
          throw new Error('temporary');
        }
      },
      { maxAttempts: 2 },
    );

    await waitFor(() => queue.getStatus(retriedId)?.state === 'completed');

    const failedId = queue.add(() => {
      throw new Error('permanent');
    });

    await waitFor(() => queue.getStatus(failedId)?.state === 'failed');

    expect(lifecycle).toEqual([
      'added',
      'started',
      'retrying',
      'started',
      'completed',
      'added',
      'started',
      'failed',
    ]);
    expect(errors).toEqual([
      { message: 'temporary', willRetry: true },
      { message: 'permanent', willRetry: false },
    ]);
    await queue.close();
  });

  test('unsubscribes event listeners and isolates listener errors', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const completed = jest.fn();
    const abortedListener = jest.fn();
    const abortController = new AbortController();
    const unsubscribe = queue.on('completed', completed);

    queue.on('completed', () => {
      throw new Error('listener failure');
    });
    queue.on('completed', abortedListener, { signal: abortController.signal });
    abortController.abort();

    const firstId = queue.add(() => 'first');

    await waitFor(() => queue.getStatus(firstId)?.state === 'completed');
    unsubscribe();

    const secondId = queue.add(() => 'second');

    await waitFor(() => queue.getStatus(secondId)?.state === 'completed');
    expect(completed).toHaveBeenCalledTimes(1);
    expect(abortedListener).not.toHaveBeenCalled();
    await queue.close();
  });

  test('runs workflow jobs after their dependencies complete', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });
    const executionOrder: string[] = [];
    const workflowId = queue.addWorkflow({
      meta: { orderId: 'order-1' },
      jobs: {
        order: {
          run: () => executionOrder.push('order'),
        },
        items: {
          dependsOn: ['order'],
          run: () => executionOrder.push('items'),
        },
        audit: {
          dependsOn: ['order'],
          run: () => executionOrder.push('audit'),
        },
        inventory: {
          dependsOn: ['items'],
          run: () => executionOrder.push('inventory'),
        },
      },
    });

    await waitFor(() => queue.getWorkflowStatus(workflowId)?.state === 'completed');

    expect(executionOrder).toEqual(['order', 'items', 'audit', 'inventory']);
    expect(queue.getWorkflowStatus(workflowId)).toMatchObject({
      state: 'completed',
      meta: { orderId: 'order-1' },
      jobs: {
        order: { state: 'completed', dependsOn: [] },
        items: { state: 'completed', dependsOn: ['order'] },
        audit: { state: 'completed', dependsOn: ['order'] },
        inventory: { state: 'completed', dependsOn: ['items'] },
      },
    });
    await queue.close();
  });

  test('skips workflow descendants when a dependency fails', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });
    const independent = jest.fn();
    const descendant = jest.fn();
    const workflowId = queue.addWorkflow({
      jobs: {
        failing: {
          run: () => {
            throw new Error('database unavailable');
          },
        },
        descendant: {
          dependsOn: ['failing'],
          run: descendant,
        },
        independent: {
          run: independent,
        },
      },
    });

    await waitFor(() => queue.getWorkflowStatus(workflowId)?.state === 'failed');

    expect(independent).toHaveBeenCalledTimes(1);
    expect(descendant).not.toHaveBeenCalled();
    expect(queue.getWorkflowStatus(workflowId)).toMatchObject({
      state: 'failed',
      jobs: {
        failing: { state: 'failed' },
        descendant: { state: 'skipped' },
        independent: { state: 'completed' },
      },
    });
    await queue.close();
  });

  test('rejects workflows with missing or cyclic dependencies', async () => {
    const queue = createQueue();

    expect(() =>
      queue.addWorkflow({
        jobs: {
          child: { dependsOn: ['missing'], run: () => undefined },
        },
      }),
    ).toThrow(RangeError);
    expect(() =>
      queue.addWorkflow({
        jobs: {
          first: { dependsOn: ['second'], run: () => undefined },
          second: { dependsOn: ['first'], run: () => undefined },
        },
      }),
    ).toThrow(RangeError);
    expect(() =>
      queue.addWorkflow({
        jobs: {
          invalid: { maxAttempts: 0, run: () => undefined },
        },
      }),
    ).toThrow(RangeError);
    expect(() =>
      queue.addWorkflow({
        jobs: {
          invalid: { timeoutMs: 0, run: () => undefined },
        },
      }),
    ).toThrow(RangeError);
    expect(() =>
      queue.addWorkflow({
        jobs: {
          invalid: { retryDelayMs: -1, run: () => undefined },
        },
      }),
    ).toThrow(RangeError);
    await queue.close();
  });

  test('drains dependent workflow jobs during graceful shutdown', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const executionOrder: string[] = [];
    const workflowId = queue.addWorkflow({
      jobs: {
        first: { run: () => executionOrder.push('first') },
        second: {
          dependsOn: ['first'],
          run: () => executionOrder.push('second'),
        },
      },
    });

    await queue.close({ drain: true });

    expect(executionOrder).toEqual(['first', 'second']);
    expect(queue.getWorkflowStatus(workflowId)?.state).toBe('completed');
  });

  test('limits concurrency', async () => {
    const queue = createQueue({ concurrency: 2, idlePollIntervalMs: 1 });

    let active = 0;
    let maximum = 0;

    const releases: Array<() => void> = [];
    const task = async (): Promise<void> => {
      active += 1;
      maximum = Math.max(maximum, active);
      await new Promise<void>((resolve) => releases.push(resolve));
      active -= 1;
    };

    queue.add(task);
    queue.add(task);
    queue.add(task);
    await waitFor(() => active === 2);
    expect(maximum).toBe(2);

    for (const release of releases.splice(0)) {
      release();
    }

    await waitFor(() => releases.length === 1);
    releases[0]?.();
    await waitFor(() => queue.getStats().completed === 3);
    expect(maximum).toBe(2);
    await queue.close();
  });

  test('applies rest intervals independently to workers', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1, restIntervalMs: 25 });
    const startedAt: number[] = [];

    queue.add(() => startedAt.push(Date.now()));
    queue.add(() => startedAt.push(Date.now()));

    await waitFor(() => queue.getStats().completed === 2);
    expect((startedAt[1] ?? 0) - (startedAt[0] ?? 0)).toBeGreaterThanOrEqual(20);
    await queue.close();
  });

  test('deduplicates active keys and releases them after completion', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });

    let release: (() => void) | undefined;

    const firstId = queue.add(() => new Promise<void>((resolve) => (release = resolve)), {
      key: 'repo:a',
    });
    const duplicateId = queue.add(() => undefined, { key: 'repo:a' });

    expect(duplicateId).toBe(firstId);
    await waitFor(() => queue.getStatus(firstId)?.state === 'running');
    release?.();
    await waitFor(() => queue.getStatus(firstId)?.state === 'completed');

    const nextId = queue.add(() => undefined, { key: 'repo:a' });

    expect(nextId).not.toBe(firstId);
    await queue.close();
  });

  test('cancels pending and running jobs', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });

    let observedAbort = false;

    const runningId = queue.add(
      ({ signal }) =>
        new Promise<void>((resolve) => {
          signal.addEventListener(
            'abort',
            () => {
              observedAbort = true;
              resolve();
            },
            { once: true },
          );
        }),
    );
    const pendingId = queue.add(() => undefined);

    await waitFor(() => queue.getStatus(runningId)?.state === 'running');
    expect(queue.cancel(pendingId)).toBe(true);
    expect(queue.cancel(runningId)).toBe(true);
    await waitFor(() => queue.getStatus(runningId)?.state === 'cancelled');

    expect(observedAbort).toBe(true);
    expect(queue.getStatus(pendingId)?.state).toBe('cancelled');
    expect(queue.cancel(runningId)).toBe(false);
    await queue.close();
  });

  test('pauses new work and resumes it', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });

    queue.pause();
    const id = queue.add(() => 'resumed');

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(queue.getStatus(id)?.state).toBe('pending');

    queue.resume();
    await waitFor(() => queue.getStatus(id)?.state === 'completed');
    expect(queue.getStatus(id)?.result).toBe('resumed');
    await queue.close();
  });

  test('can omit results and emits immutable snapshots', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1, storeResults: false });
    const snapshots: number[] = [];
    const unsubscribe = queue.subscribe(
      (snapshot) => {
        expect(Object.isFrozen(snapshot)).toBe(true);
        expect(Object.isFrozen(snapshot.jobs)).toBe(true);
        snapshots.push(snapshot.stats.total);
      },
      { immediate: true },
    );
    const id = queue.add(() => ({ secret: true }));

    await waitFor(() => queue.getStatus(id)?.state === 'completed');
    expect(queue.getStatus(id)).not.toHaveProperty('result');
    expect(snapshots.length).toBeGreaterThanOrEqual(3);
    unsubscribe();
    await queue.close();
  });

  test('supports draining and immediate shutdown modes', async () => {
    const drainingQueue = createQueue({ idlePollIntervalMs: 1 });
    const drainedId = drainingQueue.add(() => 'drained');

    await drainingQueue.close();
    expect(drainingQueue.getStatus(drainedId)?.state).toBe('completed');
    expect(() => drainingQueue.add(() => undefined)).toThrow(QueueClosedError);

    const cancellingQueue = createQueue({ idlePollIntervalMs: 100 });
    const cancelledId = cancellingQueue.add(() => undefined);

    await cancellingQueue.close({ drain: false });
    expect(cancellingQueue.getStatus(cancelledId)?.state).toBe('cancelled');
  });

  test('validates construction options', () => {
    expect(() => createQueue({ concurrency: 0 })).toThrow(RangeError);
    expect(() => createQueue({ idlePollIntervalMs: 0 })).toThrow(RangeError);
    expect(() => createQueue({ retentionMs: -1 })).toThrow(RangeError);
  });

  test('validates per-job attempt limits', async () => {
    const queue = createQueue();

    expect(() => queue.add(() => undefined, { maxAttempts: 0 })).toThrow(RangeError);
    expect(() => queue.add(() => undefined, { timeoutMs: 0 })).toThrow(RangeError);
    expect(() => queue.add(() => undefined, { retryDelayMs: -1 })).toThrow(RangeError);
    await queue.close();
  });

  test('delays a retry by a fixed retryDelayMs', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });
    const timestamps: number[] = [];
    const id = queue.add(
      ({ attempt }) => {
        timestamps.push(Date.now());

        if (attempt === 1) {
          throw new Error('temporary failure');
        }

        return 'recovered';
      },
      { maxAttempts: 2, retryDelayMs: 40 },
    );

    await waitFor(() => queue.getStatus(id)?.state === 'completed');

    expect(timestamps).toHaveLength(2);
    expect(timestamps[1] - timestamps[0]).toBeGreaterThanOrEqual(35);
    expect(queue.getStatus(id)).toMatchObject({ state: 'completed', result: 'recovered' });
    await queue.close();
  });

  test('derives the retry delay from the failed attempt number', async () => {
    const queue = createQueue({ concurrency: 1, idlePollIntervalMs: 1 });
    const seenAttempts: number[] = [];
    const retryDelayMs = jest.fn((attempt: number) => {
      seenAttempts.push(attempt);

      return 1;
    });
    const id = queue.add(
      ({ attempt }) => {
        if (attempt < 3) {
          throw new Error(`failure ${attempt}`);
        }

        return 'done';
      },
      { maxAttempts: 3, retryDelayMs },
    );

    await waitFor(() => queue.getStatus(id)?.state === 'completed');

    expect(seenAttempts).toEqual([1, 2]);
    await queue.close();
  });

  test('expires terminal jobs after the retention period using fake timers', async () => {
    jest.useFakeTimers();

    try {
      const queue = createQueue({ idlePollIntervalMs: 1, retentionMs: 100 });
      const id = queue.add(() => undefined);

      await jest.advanceTimersByTimeAsync(1);
      expect(queue.getStatus(id)?.state).toBe('completed');

      await jest.advanceTimersByTimeAsync(100);
      expect(queue.getStatus(id)).toBeUndefined();
      expect(queue.getStats().total).toBe(0);
      await queue.close();
    } finally {
      jest.useRealTimers();
    }
  });
});
