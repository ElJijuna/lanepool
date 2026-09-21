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
