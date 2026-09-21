import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const esm = await import('../dist/index.js');
const require = createRequire(import.meta.url);
const cjs = require('../dist/index.cjs');

assert.equal(typeof esm.createQueue, 'function');
assert.equal(typeof esm.createQueueMiddleware, 'function');
assert.equal(typeof cjs.createQueue, 'function');
assert.equal(typeof cjs.createQueueMiddleware, 'function');

const esmQueue = esm.createQueue({ idlePollIntervalMs: 1 });
const cjsQueue = cjs.createQueue({ idlePollIntervalMs: 1 });
await Promise.all([esmQueue.close(), cjsQueue.close()]);
