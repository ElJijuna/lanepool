import type { NextFunction, Request, Response } from 'express';
import { createQueue, createQueueMiddleware } from '../src/index.js';

describe('createQueueMiddleware', () => {
  test('attaches one provided queue and calls next', async () => {
    const queue = createQueue({ idlePollIntervalMs: 1 });
    const middleware = createQueueMiddleware(queue);
    const request = {} as Request;
    const response = {} as Response;
    const next = jest.fn() as NextFunction;

    middleware(request, response, next);

    expect(request.queue).toBe(queue);
    expect(next).toHaveBeenCalledTimes(1);
    await queue.close();
  });

  test('creates and shares a queue from options', async () => {
    const middleware = createQueueMiddleware({ concurrency: 2, idlePollIntervalMs: 1 });
    const firstRequest = {} as Request;
    const secondRequest = {} as Request;
    const next = jest.fn() as NextFunction;

    middleware(firstRequest, {} as Response, next);
    middleware(secondRequest, {} as Response, next);

    expect(firstRequest.queue).toBe(secondRequest.queue);
    expect(firstRequest.queue.getStats()).toMatchObject({ total: 0, running: 0 });
    await firstRequest.queue.close();
  });
});
