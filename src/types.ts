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
  readonly key?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
}

/** A synchronous or asynchronous unit of work. */
export type QueueTask<Result = unknown> = (context: JobContext) => Result | PromiseLike<Result>;

/** Options attached to one queued task. */
export interface AddJobOptions {
  readonly key?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
}

/** Public, immutable view of a job. */
export interface Job<Result = unknown> {
  readonly id: string;
  readonly state: JobState;
  readonly key?: string;
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

/** Options controlling graceful queue shutdown. */
export interface CloseOptions {
  readonly drain?: boolean;
}

/** Framework-neutral queue contract. */
export interface Queue {
  add<Result>(task: QueueTask<Result>, options?: AddJobOptions): string;
  getStatus<Result = unknown>(id: string): JobStatus<Result> | undefined;
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
