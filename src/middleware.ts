import type { RequestHandler } from 'express';
import { createQueue } from './queue.js';
import type { Queue, QueueOptions } from './types.js';

declare global {
  // Express exposes its request augmentation through this global namespace.
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      queue: Queue;
    }
  }
}

const isQueue = (value: Queue | QueueOptions): value is Queue =>
  'add' in value &&
  typeof value.add === 'function' &&
  'getStatus' in value &&
  typeof value.getStatus === 'function';

/** Create Express middleware backed by a shared queue instance. */
export const createQueueMiddleware = (
  optionsOrQueue: QueueOptions | Queue = {},
): RequestHandler => {
  const queue = isQueue(optionsOrQueue) ? optionsOrQueue : createQueue(optionsOrQueue);

  return (request, _response, next): void => {
    request.queue = queue;
    next();
  };
};
