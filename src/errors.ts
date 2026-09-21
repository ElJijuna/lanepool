/** Thrown when adding work after queue shutdown has started. */
export class QueueClosedError extends Error {
  public constructor() {
    super('Cannot add a job to a closed queue');
    this.name = 'QueueClosedError';
  }
}
