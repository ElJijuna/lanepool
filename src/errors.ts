/** Thrown when adding work after queue shutdown has started. */
export class QueueClosedError extends Error {
  public constructor() {
    super('Cannot add a job to a closed queue');
    this.name = 'QueueClosedError';
  }
}

/** Thrown internally when a job attempt exceeds its `timeoutMs` and is aborted. */
export class JobTimeoutError extends Error {
  public constructor(timeoutMs: number) {
    super(`Job timed out after ${timeoutMs}ms`);
    this.name = 'JobTimeoutError';
  }
}
