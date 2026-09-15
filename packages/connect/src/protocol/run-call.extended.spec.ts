// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { describe, it } from "node:test";
import * as assert from "node:assert";
import { create } from "@bufbuild/protobuf";
import { Int32ValueSchema, StringValueSchema } from "@bufbuild/protobuf/wkt";
import type { Interceptor, StreamResponse } from "../interceptor.js";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import { createContextValues } from "../context-values.js";
import { createServiceDesc } from "../descriptor-helper.spec.js";
import { createAsyncIterable } from "./async-iterable.js";
import { runStreamingCall } from "./run-call.js";

const TestService = createServiceDesc({
  typeName: "TestService",
  method: {
    serverStreaming: {
      input: Int32ValueSchema,
      output: StringValueSchema,
      methodKind: "server_streaming",
    },
  },
});

type Request = Parameters<
  typeof runStreamingCall<typeof Int32ValueSchema, typeof StringValueSchema>
>[0]["req"];

function makeReq(
  message: Request["message"] = createAsyncIterable([{ value: 123 }]),
): Request {
  return {
    stream: true,
    service: TestService,
    method: TestService.method.serverStreaming,
    requestMethod: "POST",
    url: "https://example.com/TestService/ServerStreaming",
    header: new Headers(),
    message,
    contextValues: createContextValues(),
  };
}

function makeRes(): StreamResponse<
  typeof Int32ValueSchema,
  typeof StringValueSchema
> {
  return {
    stream: true,
    service: TestService,
    method: TestService.method.serverStreaming,
    header: new Headers(),
    message: createAsyncIterable(
      ["1", "2", "3"].map((value) => create(StringValueSchema, { value })),
    ),
    trailer: new Headers(),
  };
}

function trace(outcomes: (Code | undefined)[]): Interceptor {
  return (next) => async (req) => {
    const res = await next(req);
    if (!res.stream) {
      return res;
    }
    return {
      ...res,
      message: (async function* () {
        let code: Code | undefined;
        try {
          yield* res.message;
        } catch (e) {
          code = ConnectError.from(e).code;
          throw e;
        } finally {
          outcomes.push(code);
        }
      })(),
    };
  };
}

describe("runStreamingCall() extended lifecycle", () => {
  it(
    "rejects an initial abort without starting interceptors or the transport",
    { timeout: 5_000 },
    async (t) => {
      const controller = new AbortController();
      controller.abort();
      const started = t.mock.fn();
      const returned = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const read = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const req = makeReq({
        [Symbol.asyncIterator]: () => ({ next: read, return: returned }),
      });
      await assert.rejects(
        runStreamingCall({
          req,
          signal: controller.signal,
          interceptors: [
            (next) => {
              started();
              return next;
            },
          ],
          async next() {
            started();
            return makeRes();
          },
        }),
        { code: Code.Canceled },
      );
      assert.strictEqual(started.mock.callCount(), 0);
      assert.strictEqual(read.mock.callCount(), 0);
      assert.strictEqual(returned.mock.callCount(), 1);
    },
  );

  // The minimal suite covers parked abort/deadline, parked return, and return
  // during the first read. These positions exercise the remaining races.
  for (const [action, positions] of [
    ["abort", ["unread", "first read", "pending read"]],
    ["return", ["unread", "pending read"]],
    ["throw", ["unread", "first read", "parked", "pending read"]],
    ["deadline", ["unread", "first read", "pending read"]],
  ] as const) {
    for (const position of positions) {
      it(
        `finalizes on ${action} while ${position}, retaining the terminal state after cleanup`,
        { timeout: 5_000 },
        async (t) => {
          t.mock.timers.enable({ apis: ["setTimeout"] });
          const controller = new AbortController();
          t.after(() => controller.abort());
          const outcomes: (Code | undefined)[] = [];
          const sourceClosed = t.mock.fn();
          const requestReturned = t.mock.fn(async () => ({
            done: true as const,
            value: undefined,
          }));
          const requestThrown = t.mock.fn(async (_reason: unknown) => {
            throw new Error("request cleanup failed");
          });
          const requestIterators = t.mock.fn(() => ({
            next: async () => ({ done: false as const, value: { value: 123 } }),
            return: requestReturned,
            throw: requestThrown,
          }));
          let reads = 0;
          let readStarted = () => {};
          const reading = new Promise<void>((resolve) => {
            readStarted = resolve;
          });
          const error = new ConnectError("consumer failed", Code.DataLoss);
          const cleanReturn = action === "return" && position === "unread";
          const code = cleanReturn
            ? undefined
            : action === "deadline"
              ? Code.DeadlineExceeded
              : action === "throw"
                ? Code.DataLoss
                : Code.Canceled;
          const res = await runStreamingCall({
            req: makeReq({ [Symbol.asyncIterator]: requestIterators }),
            signal: controller.signal,
            timeoutMs: 100,
            interceptors: [trace(outcomes)],
            async next(request) {
              await request.message[Symbol.asyncIterator]().next();
              return {
                ...makeRes(),
                message: (async function* () {
                  try {
                    if (position !== "first read") {
                      reads++;
                      yield create(StringValueSchema, { value: "first" });
                    }
                    reads++;
                    readStarted();
                    if (!request.signal.aborted) {
                      await new Promise<void>((resolve) =>
                        request.signal.addEventListener(
                          "abort",
                          () => resolve(),
                          {
                            once: true,
                          },
                        ),
                      );
                    }
                    // Some transports finish with successful EOF on abort.
                  } finally {
                    sourceClosed();
                  }
                })(),
              };
            },
          });
          const iterator = res.message[Symbol.asyncIterator]();
          assert.strictEqual(reads, 0);
          if (position === "parked" || position === "pending read") {
            assert.deepStrictEqual(
              (await iterator.next()).value,
              create(StringValueSchema, { value: "first" }),
            );
            assert.strictEqual(reads, 1);
            assert.deepStrictEqual(outcomes, []);
          }
          const pending =
            position === "first read" || position === "pending read"
              ? assert.rejects(iterator.next(), { code })
              : undefined;
          if (pending) {
            await reading;
          }
          if (action === "deadline") {
            t.mock.timers.tick(100);
          } else if (action === "abort") {
            controller.abort();
          } else if (action === "return") {
            assert.ok(iterator.return);
            await iterator.return();
          } else {
            assert.ok(iterator.throw);
            await assert.rejects(
              iterator.throw(error),
              (reason) => reason === error,
            );
          }
          await new Promise((resolve) => setImmediate(resolve));
          assert.deepStrictEqual(outcomes, [code]);
          assert.strictEqual(
            sourceClosed.mock.callCount(),
            position === "unread" ? 0 : 1,
          );
          assert.strictEqual(requestIterators.mock.callCount(), 1);
          assert.strictEqual(requestReturned.mock.callCount(), 1);
          assert.strictEqual(
            requestThrown.mock.callCount(),
            cleanReturn ? 0 : 1,
          );
          if (cleanReturn) {
            assert.deepStrictEqual(await iterator.next(), {
              done: true,
              value: undefined,
            });
          } else {
            await assert.rejects(iterator.next(), { code });
            assert.strictEqual(
              ConnectError.from(requestThrown.mock.calls[0].arguments[0]).code,
              code,
            );
          }
          await iterator.return?.();
          await iterator.return?.();
          assert.deepStrictEqual(outcomes, [code]);
          assert.strictEqual(requestReturned.mock.callCount(), 1);
          assert.strictEqual(
            requestThrown.mock.callCount(),
            cleanReturn ? 0 : 1,
          );
          await pending;
        },
      );
    }
  }

  for (const action of ["return", "abort"] as const) {
    it(
      `reports ${action} through a buffering interceptor without prefetching`,
      { timeout: 5_000 },
      async (t) => {
        const controller = new AbortController();
        const outcomes: (Code | undefined)[] = [];
        const sourceClosed = t.mock.fn();
        let sourceReads = 0;
        const duplicate: Interceptor = (next) => async (req) => {
          const res = await next(req);
          if (!res.stream) {
            return res;
          }
          return {
            ...res,
            message: (async function* () {
              for await (const message of res.message) {
                for (let i = 0; i < 3; i++) {
                  yield message;
                }
              }
            })(),
          };
        };
        const res = await runStreamingCall({
          req: makeReq(),
          signal: controller.signal,
          interceptors: [trace(outcomes), duplicate],
          async next() {
            return {
              ...makeRes(),
              message: (async function* () {
                try {
                  for (const value of ["first", "second"]) {
                    sourceReads++;
                    yield create(StringValueSchema, { value });
                  }
                } finally {
                  sourceClosed();
                }
              })(),
            };
          },
        });
        const iterator = res.message[Symbol.asyncIterator]();
        assert.strictEqual(sourceReads, 0);
        assert.deepStrictEqual(
          (await iterator.next()).value,
          create(StringValueSchema, { value: "first" }),
        );
        assert.strictEqual(sourceReads, 1);
        if (action === "abort") {
          controller.abort();
        }
        assert.ok(iterator.return);
        await iterator.return();
        assert.deepStrictEqual(outcomes, [
          action === "return" ? undefined : Code.Canceled,
        ]);
        assert.strictEqual(sourceReads, 1);
        assert.strictEqual(sourceClosed.mock.callCount(), 1);
        if (action === "return") {
          assert.deepStrictEqual(await iterator.next(), {
            done: true,
            value: undefined,
          });
        } else {
          await assert.rejects(iterator.next(), { code: Code.Canceled });
        }
      },
    );
  }

  it(
    "keeps a for-await break successful when response cleanup rejects",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const outcomes: (Code | undefined)[] = [];
      const returned = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const thrown = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      let signal: AbortSignal | undefined;
      const read = t.mock.fn(async () => ({
        done: false as const,
        value: create(StringValueSchema),
      }));
      const sourceReturned = t.mock.fn(() => Promise.reject(signal?.reason));
      const res = await runStreamingCall({
        req: makeReq({
          [Symbol.asyncIterator]: () => ({
            next: async () => ({ done: true, value: undefined }),
            return: returned,
            throw: thrown,
          }),
        }),
        timeoutMs: 100,
        interceptors: [trace(outcomes)],
        async next(req) {
          signal = req.signal;
          return {
            ...makeRes(),
            message: {
              [Symbol.asyncIterator]: () => ({
                next: read,
                return: sourceReturned,
              }),
            },
          };
        },
      });
      for await (const message of res.message) {
        assert.deepStrictEqual(message, create(StringValueSchema));
        break;
      }
      const iterator = res.message[Symbol.asyncIterator]();
      await iterator.return?.();
      t.mock.timers.tick(100);
      assert.deepStrictEqual(outcomes, [undefined]);
      assert.strictEqual(signal?.aborted, true);
      assert.strictEqual(cleared.mock.callCount(), 1);
      assert.strictEqual(returned.mock.callCount(), 1);
      assert.strictEqual(thrown.mock.callCount(), 0);
      assert.strictEqual(read.mock.callCount(), 1);
      assert.strictEqual(sourceReturned.mock.callCount(), 1);
      assert.deepStrictEqual(await iterator.next(), {
        done: true,
        value: undefined,
      });
    },
  );

  it(
    "cancels return when one of multiple public reads is still pending",
    { timeout: 5_000 },
    async () => {
      const res = await runStreamingCall({
        req: makeReq(),
        interceptors: [
          (next) => async (req) => {
            const res = await next(req);
            if (!res.stream) {
              return res;
            }
            const source = res.message[Symbol.asyncIterator]();
            let reads = 0;
            return {
              ...res,
              message: {
                [Symbol.asyncIterator]: () => ({
                  next() {
                    reads++;
                    return reads === 2
                      ? Promise.resolve({
                          done: false,
                          value: create(req.method.output),
                        })
                      : source.next();
                  },
                }),
              },
            };
          },
        ],
        async next(req) {
          return {
            ...makeRes(),
            message: (async function* () {
              if (!req.signal.aborted) {
                await new Promise<void>((resolve) =>
                  req.signal.addEventListener("abort", () => resolve(), {
                    once: true,
                  }),
                );
              }
            })(),
          };
        },
      });
      const iterator = res.message[Symbol.asyncIterator]();
      const pending = assert.rejects(iterator.next(), { code: Code.Canceled });
      assert.strictEqual((await iterator.next()).done, false);
      assert.ok(iterator.return);
      await iterator.return();
      await pending;
      await assert.rejects(iterator.next(), { code: Code.Canceled });
    },
  );

  it(
    "keeps normal completion successful and finalizes the consumed request iterator once",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const returned = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const thrown = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const iterators = t.mock.fn(() => ({
        next: async () => ({ done: false as const, value: { value: 123 } }),
        return: returned,
        throw: thrown,
      }));
      const outcomes: (Code | undefined)[] = [];
      const controller = new AbortController();
      const res = await runStreamingCall({
        req: makeReq({ [Symbol.asyncIterator]: iterators }),
        signal: controller.signal,
        timeoutMs: 100,
        interceptors: [trace(outcomes)],
        async next(request) {
          const input = request.message[Symbol.asyncIterator]();
          assert.strictEqual((await input.next()).value.value, 123);
          await input.return?.();
          return makeRes();
        },
      });
      const values = [];
      for await (const message of res.message) {
        values.push(message.value);
      }
      assert.deepStrictEqual(values, ["1", "2", "3"]);
      controller.abort();
      t.mock.timers.tick(100);
      const iterator = res.message[Symbol.asyncIterator]();
      assert.deepStrictEqual(await iterator.next(), {
        done: true,
        value: undefined,
      });
      await iterator.return?.();
      assert.deepStrictEqual(outcomes, [undefined]);
      assert.strictEqual(iterators.mock.callCount(), 1);
      assert.strictEqual(returned.mock.callCount(), 1);
      assert.strictEqual(thrown.mock.callCount(), 0);
      assert.strictEqual(cleared.mock.callCount(), 1);
    },
  );

  for (const frames of [0, 1]) {
    it(
      `preserves response errors after ${frames} frames independently of trailers`,
      { timeout: 5_000 },
      async () => {
        const error = new ConnectError("broken frame", Code.InvalidArgument, {
          "service-header": "value",
        });
        error.details = [
          { type: "example.Detail", value: Uint8Array.of(8, 7) },
        ];
        const outcomes: (Code | undefined)[] = [];
        const res = await runStreamingCall({
          req: makeReq(),
          interceptors: [trace(outcomes)],
          async next() {
            return {
              ...makeRes(),
              trailer: new Headers({ "grpc-status": "0" }),
              message: (async function* () {
                if (frames) {
                  yield create(StringValueSchema);
                }
                throw error;
              })(),
            };
          },
        });
        const iterator = res.message[Symbol.asyncIterator]();
        if (frames) {
          assert.strictEqual((await iterator.next()).done, false);
        }
        await assert.rejects(iterator.next(), (reason) => reason === error);
        await iterator.return?.();
        await assert.rejects(iterator.next(), (reason) => reason === error);
        assert.strictEqual(error.metadata.get("service-header"), "value");
        assert.deepStrictEqual(error.details, [
          { type: "example.Detail", value: Uint8Array.of(8, 7) },
        ]);
        assert.deepStrictEqual(outcomes, [Code.InvalidArgument]);
      },
    );
  }

  it(
    "keeps concurrent reads of successful EOF successful",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const res = await runStreamingCall({
        req: makeReq(),
        timeoutMs: 100,
        async next() {
          return { ...makeRes(), message: createAsyncIterable([]) };
        },
      });
      const iterator = res.message[Symbol.asyncIterator]();
      assert.deepStrictEqual(
        await Promise.all([iterator.next(), iterator.next()]),
        [
          { done: true, value: undefined },
          { done: true, value: undefined },
        ],
      );
      assert.strictEqual(cleared.mock.callCount(), 1);
    },
  );

  it(
    "preserves a queued read error after another read completes successfully",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const error = new ConnectError("late read failure", Code.DataLoss, {
        "service-trailer": "value",
      });
      error.details = [{ type: "example.Detail", value: Uint8Array.of(8, 7) }];
      let rejectRead = (_reason: unknown) => {};
      const firstRead = new Promise<never>((_, reject) => {
        rejectRead = reject;
      });
      const res = await runStreamingCall({
        req: makeReq(),
        timeoutMs: 100,
        interceptors: [
          (next) => async (req) => {
            const res = await next(req);
            if (!res.stream) {
              return res;
            }
            return {
              ...res,
              message: {
                [Symbol.asyncIterator]() {
                  let reads = 0;
                  return {
                    next() {
                      reads++;
                      return reads === 1
                        ? firstRead
                        : Promise.resolve({ done: true, value: undefined });
                    },
                  };
                },
              },
            };
          },
        ],
        async next() {
          return makeRes();
        },
      });
      const iterator = res.message[Symbol.asyncIterator]();
      const read = iterator.next();
      const pending = assert.rejects(read, (reason) => reason === error);
      assert.deepStrictEqual(await iterator.next(), {
        done: true,
        value: undefined,
      });
      rejectRead(error);
      await pending;
      assert.strictEqual(error.metadata.get("service-trailer"), "value");
      assert.deepStrictEqual(error.details, [
        { type: "example.Detail", value: Uint8Array.of(8, 7) },
      ]);
      assert.deepStrictEqual(await iterator.next(), {
        done: true,
        value: undefined,
      });
      assert.strictEqual(cleared.mock.callCount(), 1);
    },
  );

  it(
    "finalizes a response received after cancellation during setup",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const controller = new AbortController();
      const outcomes: (Code | undefined)[] = [];
      const returned = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const read = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      await assert.rejects(
        runStreamingCall({
          req: makeReq(),
          signal: controller.signal,
          timeoutMs: 100,
          interceptors: [trace(outcomes)],
          async next() {
            controller.abort();
            return {
              ...makeRes(),
              message: {
                [Symbol.asyncIterator]: () => ({
                  next: read,
                  return: returned,
                }),
              },
            };
          },
        }),
        { code: Code.Canceled },
      );
      assert.deepStrictEqual(outcomes, [Code.Canceled]);
      assert.strictEqual(read.mock.callCount(), 0);
      assert.strictEqual(returned.mock.callCount(), 1);
      assert.strictEqual(cleared.mock.callCount(), 1);
    },
  );

  it(
    "reports cancellation to interceptors when the transport loses the abort reason",
    { timeout: 5_000 },
    async () => {
      const controller = new AbortController();
      const failures: Code[] = [];
      await assert.rejects(
        runStreamingCall({
          req: makeReq(),
          signal: controller.signal,
          interceptors: [
            (next) => async (req) => {
              try {
                return await next(req);
              } catch (e) {
                failures.push(ConnectError.from(e).code);
                throw e;
              }
            },
          ],
          async next() {
            controller.abort();
            throw new TypeError("fetch failed");
          },
        }),
        { code: Code.Canceled },
      );
      assert.deepStrictEqual(failures, [Code.Canceled]);
    },
  );

  it(
    "releases the request and deadline when an interceptor factory throws",
    { timeout: 5_000 },
    async (t) => {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      const cleared = t.mock.method(globalThis, "clearTimeout");
      const returned = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const read = t.mock.fn(async () => ({
        done: true as const,
        value: undefined,
      }));
      const next = t.mock.fn(async () => makeRes());
      const error = new ConnectError("interceptor failed", Code.Internal);
      await assert.rejects(
        runStreamingCall({
          req: makeReq({
            [Symbol.asyncIterator]: () => ({ next: read, return: returned }),
          }),
          timeoutMs: 100,
          interceptors: [
            () => {
              throw error;
            },
          ],
          next,
        }),
        (reason) => reason === error,
      );
      assert.strictEqual(next.mock.callCount(), 0);
      assert.strictEqual(read.mock.callCount(), 0);
      assert.strictEqual(returned.mock.callCount(), 1);
      assert.strictEqual(cleared.mock.callCount(), 1);
    },
  );

  it(
    "does not replace cancellation with a response cleanup error",
    { timeout: 5_000 },
    async (t) => {
      const controller = new AbortController();
      const outcomes: (Code | undefined)[] = [];
      const returned = t.mock.fn(async () => {
        throw new Error("cleanup failed");
      });
      const res = await runStreamingCall({
        req: makeReq(),
        signal: controller.signal,
        interceptors: [trace(outcomes)],
        async next() {
          return {
            ...makeRes(),
            message: {
              [Symbol.asyncIterator]: () => ({
                next: async () => ({
                  done: false,
                  value: create(StringValueSchema),
                }),
                return: returned,
              }),
            },
          };
        },
      });
      const iterator = res.message[Symbol.asyncIterator]();
      await iterator.next();
      controller.abort();
      await iterator.return?.();
      await assert.rejects(iterator.next(), { code: Code.Canceled });
      assert.deepStrictEqual(outcomes, [Code.Canceled]);
      assert.strictEqual(returned.mock.callCount(), 1);
    },
  );

  it(
    "reports response cleanup failure after successful EOF to interceptors",
    { timeout: 5_000 },
    async (t) => {
      const error = new ConnectError("cleanup failed", Code.Unavailable);
      const returned = t.mock.fn(async () => {
        throw error;
      });
      const outcomes: (Code | undefined)[] = [];
      const res = await runStreamingCall({
        req: makeReq(),
        interceptors: [trace(outcomes)],
        async next() {
          return {
            ...makeRes(),
            message: {
              [Symbol.asyncIterator]: () => ({
                next: async () => ({ done: true, value: undefined }),
                return: returned,
              }),
            },
          };
        },
      });
      const iterator = res.message[Symbol.asyncIterator]();
      await assert.rejects(iterator.next(), (reason) => reason === error);
      await iterator.return?.();
      await assert.rejects(iterator.next(), (reason) => reason === error);
      assert.deepStrictEqual(outcomes, [Code.Unavailable]);
      assert.strictEqual(returned.mock.callCount(), 1);
    },
  );
});
