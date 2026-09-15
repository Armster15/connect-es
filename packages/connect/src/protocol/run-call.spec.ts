// Copyright 2021-2025 The Connect Authors
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

import { create } from "@bufbuild/protobuf";
import { runStreamingCall, runUnaryCall } from "./run-call.js";
import type {
  Interceptor,
  StreamRequest,
  StreamResponse,
  UnaryRequest,
  UnaryResponse,
} from "../interceptor.js";
import { createAsyncIterable } from "./async-iterable.js";
import { createContextValues } from "../context-values.js";
import { createServiceDesc } from "../descriptor-helper.spec.js";
import { Int32ValueSchema, StringValueSchema } from "@bufbuild/protobuf/wkt";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";

const TestService = createServiceDesc({
  typeName: "TestService",
  method: {
    unary: {
      input: Int32ValueSchema,
      output: StringValueSchema,
      methodKind: "unary",
    },
    serverStreaming: {
      input: Int32ValueSchema,
      output: StringValueSchema,
      methodKind: "server_streaming",
    },
  },
});

describe("runUnaryCall()", () => {
  function makeReq() {
    return {
      stream: false as const,
      service: TestService,
      method: TestService.method.unary,
      requestMethod: "POST",
      url: `https://example.com/TestService/Unary`,
      header: new Headers(),
      message: { value: 123 },
      contextValues: createContextValues(),
    };
  }

  function makeRes(
    req: UnaryRequest<typeof Int32ValueSchema, typeof StringValueSchema>,
  ) {
    return <UnaryResponse<typeof Int32ValueSchema, typeof StringValueSchema>>{
      stream: false,
      service: TestService,
      method: TestService.method.unary,
      header: new Headers(),
      message: create(StringValueSchema, {
        value: req.message.value.toString(10),
      }),
      trailer: new Headers(),
    };
  }
  it("should return the response", async () => {
    const res = await runUnaryCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      timeoutMs: undefined,
      signal: undefined,
      interceptors: [],
      req: makeReq(),
      async next(req) {
        await new Promise((resolve) => setTimeout(resolve, 1));
        return makeRes(req);
      },
    });
    expect(res.message.value).toBe("123");
  });
  it("should trigger the signal when done", async () => {
    let signal: AbortSignal | undefined;
    await runUnaryCall<typeof Int32ValueSchema, typeof StringValueSchema>({
      req: makeReq(),
      async next(req) {
        signal = req.signal;
        await new Promise((resolve) => setTimeout(resolve, 1));
        return makeRes(req);
      },
    });
    expect(signal?.aborted).toBeTrue();
  });
  it("should raise Code.Canceled on user abort", async () => {
    const userAbort = new AbortController();
    const resPromise = runUnaryCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      signal: userAbort.signal,
      req: makeReq(),
      async next(req) {
        for (;;) {
          await new Promise((resolve) => setTimeout(resolve, 1));
          req.signal.throwIfAborted();
        }
      },
    });
    userAbort.abort();
    await expectAsync(resPromise).toBeRejectedWithError(
      "[canceled] This operation was aborted",
    );
  });
  it("should raise Code.DeadlineExceeded on timeout", async () => {
    const resPromise = runUnaryCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      timeoutMs: 1,
      req: makeReq(),
      async next(req) {
        for (;;) {
          await new Promise((resolve) => setTimeout(resolve, 1));
          req.signal.throwIfAborted();
        }
      },
    });
    await expectAsync(resPromise).toBeRejectedWithError(
      "[deadline_exceeded] the operation timed out",
    );
  });
});

describe("runStreamingCall()", () => {
  function makeReq() {
    return {
      stream: true as const,
      service: TestService,
      method: TestService.method.serverStreaming,
      requestMethod: "POST",
      url: `https://example.com/TestService/ServerStreaming`,
      header: new Headers(),
      message: createAsyncIterable([{ value: 1 }, { value: 2 }, { value: 3 }]),
      contextValues: createContextValues(),
    };
  }

  function makeRes(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    req: StreamRequest<typeof Int32ValueSchema, typeof StringValueSchema>,
  ) {
    return <StreamResponse<typeof Int32ValueSchema, typeof StringValueSchema>>{
      stream: true,
      service: TestService,
      method: TestService.method.serverStreaming,
      header: new Headers(),
      message: createAsyncIterable([
        create(StringValueSchema, { value: "1" }),
        create(StringValueSchema, { value: "2" }),
        create(StringValueSchema, { value: "3" }),
      ]),
      trailer: new Headers(),
    };
  }

  it("should return the response", async () => {
    const req = makeReq();
    const res = await runStreamingCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      timeoutMs: undefined,
      signal: undefined,
      interceptors: [],
      req: req,
      async next(req) {
        await new Promise((resolve) => setTimeout(resolve, 1));
        return makeRes(req);
      },
    });
    const values: string[] = [];
    for await (const m of res.message) {
      values.push(m.value);
    }
    expect(values).toEqual(["1", "2", "3"]);
    const it = req.message[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ done: true, value: undefined });
    const resIt = res.message[Symbol.asyncIterator]();
    expect(resIt.throw).toBeDefined(); // eslint-disable-line  @typescript-eslint/unbound-method
    expect(resIt.return).toBeDefined(); // eslint-disable-line  @typescript-eslint/unbound-method
  });
  it("should trigger the signal when done", async () => {
    let signal: AbortSignal | undefined;
    const req = makeReq();
    const res = await runStreamingCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      req: req,
      async next(req) {
        signal = req.signal;
        await new Promise((resolve) => setTimeout(resolve, 1));
        return makeRes(req);
      },
    });
    for await (const m of res.message) {
      expect(m).toBeDefined();
    }
    expect(signal?.aborted).toBeTrue();
    const it = req.message[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ done: true, value: undefined });
  });
  it("should raise Code.Canceled on user abort", async () => {
    const userAbort = new AbortController();
    const req = makeReq();
    const resPromise = runStreamingCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      signal: userAbort.signal,
      req: req,
      async next(req) {
        for (;;) {
          await new Promise((resolve) => setTimeout(resolve, 1));
          req.signal.throwIfAborted();
        }
      },
    });
    userAbort.abort();
    await expectAsync(resPromise).toBeRejectedWithError(
      "[canceled] This operation was aborted",
    );
    const it = req.message[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ done: true, value: undefined });
  });
  it("should raise Code.DeadlineExceeded on timeout", async () => {
    const req = makeReq();
    const resPromise = runStreamingCall<
      typeof Int32ValueSchema,
      typeof StringValueSchema
    >({
      timeoutMs: 1,
      req: req,
      async next(req) {
        for (;;) {
          await new Promise((resolve) => setTimeout(resolve, 1));
          req.signal.throwIfAborted();
        }
      },
    });
    await expectAsync(resPromise).toBeRejectedWithError(
      "[deadline_exceeded] the operation timed out",
    );
    const it = req.message[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ done: true, value: undefined });
  });
  it("should propagate the error thrown in next", async () => {
    const req = makeReq();
    let reqError: Error | undefined;
    req.message = {
      [Symbol.asyncIterator]() {
        return {
          next() {
            fail("unexpected call");
            throw new Error("unexpected call");
          },
          throw(e) {
            reqError = e as Error;
            return Promise.reject({ done: true, value: undefined });
          },
        };
      },
    };
    await expectAsync(
      runStreamingCall<typeof Int32ValueSchema, typeof StringValueSchema>({
        req: req,
        next() {
          return Promise.reject(new Error("foo"));
        },
      }),
    ).toBeRejectedWithError("[unknown] foo");
    expect(reqError?.message).toEqual("[unknown] foo");
  });

  describe("response lifecycle", () => {
    function trace(finished: (code: Code | undefined) => void): Interceptor {
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
              finished(code);
            }
          })(),
        };
      };
    }

    it("rejects an initial abort without starting interceptors or the transport", async () => {
      const controller = new AbortController();
      controller.abort();
      const started = jasmine.createSpy("started");
      const req = makeReq();
      const returned = jasmine
        .createSpy("returned")
        .and.resolveTo({ done: true });
      req.message = {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.resolve({ done: true, value: undefined }),
          return: returned,
        }),
      };
      await expectAsync(
        runStreamingCall({
          req,
          signal: controller.signal,
          interceptors: [
            (next) => (request) => {
              started();
              return next(request);
            },
          ],
          async next(request) {
            started();
            return makeRes(request);
          },
        }),
      ).toBeRejectedWith(jasmine.objectContaining({ code: Code.Canceled }));
      expect(started).not.toHaveBeenCalled();
      expect(returned).toHaveBeenCalledTimes(1);
    });

    for (const action of ["abort", "return", "throw", "deadline"] as const) {
      for (const position of [
        "unread",
        "first read",
        "parked",
        "pending read",
      ] as const) {
        it(`finalizes on ${action} while ${position}, retaining the terminal state after cleanup`, async () => {
          const controller = new AbortController();
          const finished = jasmine.createSpy("finished");
          const sourceClosed = jasmine.createSpy("sourceClosed");
          const requestReturned = jasmine
            .createSpy("requestReturned")
            .and.resolveTo({ done: true });
          const requestThrown = jasmine
            .createSpy("requestThrown")
            .and.rejectWith(new Error("cleanup"));
          const req = makeReq();
          const requestIterators = jasmine
            .createSpy("requestIterators")
            .and.callFake(() => ({
              next: () =>
                Promise.resolve({ done: false, value: { value: 123 } }),
              return: requestReturned,
              throw: requestThrown,
            }));
          req.message = { [Symbol.asyncIterator]: requestIterators };
          let reads = 0;
          let readStarted = () => {};
          const reading = new Promise<void>((resolve) => {
            readStarted = resolve;
          });
          const error = new ConnectError("consumer failed", Code.DataLoss);
          const cleanReturn =
            action === "return" &&
            (position === "unread" || position === "parked");
          const code = cleanReturn
            ? undefined
            : action === "deadline"
              ? Code.DeadlineExceeded
              : action === "throw"
                ? Code.DataLoss
                : Code.Canceled;
          jasmine.clock().install();
          try {
            const res = await runStreamingCall({
              req,
              signal: controller.signal,
              timeoutMs: 100,
              interceptors: [trace(finished)],
              async next(request) {
                await request.message[Symbol.asyncIterator]().next();
                return {
                  ...makeRes(request),
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
                            { once: true },
                          ),
                        );
                      }
                      // A transport may finish with successful EOF on abort.
                    } finally {
                      sourceClosed();
                    }
                  })(),
                };
              },
            });
            const it = res.message[Symbol.asyncIterator]();
            expect(reads).toBe(0);
            if (position === "parked" || position === "pending read") {
              expect((await it.next()).value).toEqual(
                create(StringValueSchema, { value: "first" }),
              );
              expect(reads).toBe(1);
              expect(finished).not.toHaveBeenCalled();
            }
            const pending =
              position === "first read" || position === "pending read"
                ? expectAsync(it.next()).toBeRejectedWith(
                    jasmine.objectContaining({ code }),
                  )
                : undefined;
            if (pending) {
              await reading;
            }
            let closing: PromiseLike<unknown> | undefined;
            if (action === "deadline") {
              jasmine.clock().tick(100);
            } else if (action === "abort") {
              controller.abort();
            } else if (action === "return") {
              if (!it.return) {
                throw new Error("response iterator must provide return()");
              }
              closing = it.return();
            } else {
              if (!it.throw) {
                throw new Error("response iterator must provide throw()");
              }
              closing = expectAsync(it.throw(error)).toBeRejectedWith(error);
            }
            jasmine.clock().uninstall();
            await closing;
            await new Promise((resolve) => setTimeout(resolve, 0));
            expect(finished).toHaveBeenCalledOnceWith(code);
            if (position !== "unread") {
              expect(sourceClosed).toHaveBeenCalledTimes(1);
            }
            expect(requestIterators).toHaveBeenCalledTimes(1);
            expect(requestReturned).toHaveBeenCalledTimes(1);
            expect(requestThrown).toHaveBeenCalledTimes(cleanReturn ? 0 : 1);
            if (cleanReturn) {
              expect(await it.next()).toEqual({ done: true, value: undefined });
            } else {
              await expectAsync(it.next()).toBeRejectedWith(
                jasmine.objectContaining({ code }),
              );
            }
            await it.return?.();
            await it.return?.();
            expect(finished).toHaveBeenCalledTimes(1);
            expect(requestReturned).toHaveBeenCalledTimes(1);
            await pending;
          } finally {
            controller.abort();
            jasmine.clock().uninstall();
          }
        }, 1000);
      }
    }

    for (const action of ["return", "abort"] as const) {
      it(`reports ${action} through a buffering interceptor without prefetching`, async () => {
        const controller = new AbortController();
        const finished = jasmine.createSpy("finished");
        const sourceClosed = jasmine.createSpy("sourceClosed");
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
          interceptors: [trace(finished), duplicate],
          async next(req) {
            return {
              ...makeRes(req),
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
        const it = res.message[Symbol.asyncIterator]();
        expect(sourceReads).toBe(0);
        expect((await it.next()).value).toEqual(
          create(StringValueSchema, { value: "first" }),
        );
        expect(sourceReads).toBe(1);
        if (action === "abort") {
          controller.abort();
        }
        await it.return?.();
        expect(finished).toHaveBeenCalledOnceWith(
          action === "return" ? undefined : Code.Canceled,
        );
        expect(sourceReads).toBe(1);
        expect(sourceClosed).toHaveBeenCalledTimes(1);
        if (action === "return") {
          expect(await it.next()).toEqual({ done: true, value: undefined });
        } else {
          await expectAsync(it.next()).toBeRejectedWith(
            jasmine.objectContaining({ code: Code.Canceled }),
          );
        }
      });
    }

    it("closes a for-await break cleanly and releases resources without another read", async () => {
      const finished = jasmine.createSpy("finished");
      const returned = jasmine
        .createSpy("returned")
        .and.resolveTo({ done: true });
      const thrown = jasmine.createSpy("thrown").and.resolveTo({ done: true });
      const request = makeReq();
      request.message = {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.resolve({ done: true, value: undefined }),
          return: returned,
          throw: thrown,
        }),
      };
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
      let signal: AbortSignal | undefined;
      const read = jasmine
        .createSpy("read")
        .and.resolveTo({ done: false, value: create(StringValueSchema) });
      const sourceReturned = jasmine
        .createSpy("sourceReturned")
        .and.callFake(() => Promise.reject(signal?.reason));
      const res = await runStreamingCall({
        req: request,
        timeoutMs: 100,
        interceptors: [trace(finished)],
        async next(req) {
          signal = req.signal;
          return {
            ...makeRes(req),
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
        expect(message).toEqual(create(StringValueSchema));
        break;
      }
      const it = res.message[Symbol.asyncIterator]();
      await it.return?.();
      expect(finished).toHaveBeenCalledOnceWith(undefined);
      expect(signal?.aborted).toBeTrue();
      expect(cleared).toHaveBeenCalledTimes(1);
      expect(returned).toHaveBeenCalledTimes(1);
      expect(thrown).not.toHaveBeenCalled();
      expect(read).toHaveBeenCalledTimes(1);
      expect(sourceReturned).toHaveBeenCalledTimes(1);
      expect(await it.next()).toEqual({ done: true, value: undefined });
    });

    it("cancels return when one of multiple public reads is still pending", async () => {
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
            ...makeRes(req),
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
      const it = res.message[Symbol.asyncIterator]();
      const pending = expectAsync(it.next()).toBeRejectedWith(
        jasmine.objectContaining({ code: Code.Canceled }),
      );
      expect((await it.next()).done).toBeFalse();
      await it.return?.();
      await pending;
      await expectAsync(it.next()).toBeRejectedWith(
        jasmine.objectContaining({ code: Code.Canceled }),
      );
    });

    it("keeps normal completion successful and finalizes the consumed request iterator once", async () => {
      const req = makeReq();
      const returned = jasmine
        .createSpy("returned")
        .and.resolveTo({ done: true });
      const thrown = jasmine.createSpy("thrown");
      const iterators = jasmine.createSpy("iterators").and.callFake(() => ({
        next: () => Promise.resolve({ done: false, value: { value: 123 } }),
        return: returned,
        throw: thrown,
      }));
      req.message = { [Symbol.asyncIterator]: iterators };
      const finished = jasmine.createSpy("finished");
      const controller = new AbortController();
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
      const res = await runStreamingCall({
        req,
        signal: controller.signal,
        timeoutMs: 100,
        interceptors: [trace(finished)],
        async next(request) {
          const input = request.message[Symbol.asyncIterator]();
          await input.next();
          await input.return?.();
          return makeRes(request);
        },
      });
      const values = [];
      for await (const message of res.message) {
        values.push(message.value);
      }
      expect(values).toEqual(["1", "2", "3"]);
      controller.abort();
      const it = res.message[Symbol.asyncIterator]();
      expect(await it.next()).toEqual({ done: true, value: undefined });
      await it.return?.();
      expect(finished).toHaveBeenCalledOnceWith(undefined);
      expect(iterators).toHaveBeenCalledTimes(1);
      expect(returned).toHaveBeenCalledTimes(1);
      expect(thrown).not.toHaveBeenCalled();
      expect(cleared).toHaveBeenCalledTimes(1);
    });

    for (const frames of [0, 1]) {
      it(`preserves response errors after ${frames} frames independently of trailers`, async () => {
        const error = new ConnectError("broken frame", Code.InvalidArgument, {
          "service-header": "value",
        });
        const finished = jasmine.createSpy("finished");
        const res = await runStreamingCall({
          req: makeReq(),
          interceptors: [trace(finished)],
          async next(req) {
            return {
              ...makeRes(req),
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
        const it = res.message[Symbol.asyncIterator]();
        if (frames) {
          expect((await it.next()).done).not.toBeTrue();
        }
        await expectAsync(it.next()).toBeRejectedWith(error);
        await expectAsync(it.next()).toBeRejectedWith(error);
        await it.return?.();
        expect(finished).toHaveBeenCalledOnceWith(Code.InvalidArgument);
      });
    }

    it("keeps concurrent reads of successful EOF successful", async () => {
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
      const res = await runStreamingCall({
        req: makeReq(),
        timeoutMs: 100,
        async next(req) {
          return { ...makeRes(req), message: createAsyncIterable([]) };
        },
      });
      const it = res.message[Symbol.asyncIterator]();
      expect(await Promise.all([it.next(), it.next()])).toEqual([
        { done: true, value: undefined },
        { done: true, value: undefined },
      ]);
      expect(cleared).toHaveBeenCalledTimes(1);
    });

    it("preserves a queued read error after another read completes successfully", async () => {
      const error = new ConnectError("late read failure", Code.DataLoss, {
        "service-trailer": "value",
      });
      error.details = [{ type: "example.Detail", value: Uint8Array.of(8, 7) }];
      let rejectRead = (_reason: unknown) => {};
      const firstRead = new Promise<never>((_, reject) => {
        rejectRead = reject;
      });
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
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
        async next(req) {
          return makeRes(req);
        },
      });
      const it = res.message[Symbol.asyncIterator]();
      const read = it.next();
      const pending = expectAsync(read).toBeRejectedWith(error);
      expect(await it.next()).toEqual({ done: true, value: undefined });
      rejectRead(error);
      await pending;
      expect(await read.catch((reason: unknown) => reason)).toBe(error);
      expect(await it.next()).toEqual({ done: true, value: undefined });
      expect(cleared).toHaveBeenCalledTimes(1);
    });

    it("finalizes a response received after cancellation during setup", async () => {
      const controller = new AbortController();
      const finished = jasmine.createSpy("finished");
      const returned = jasmine
        .createSpy("returned")
        .and.resolveTo({ done: true });
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
      await expectAsync(
        runStreamingCall({
          req: makeReq(),
          signal: controller.signal,
          timeoutMs: 100,
          interceptors: [trace(finished)],
          async next(req) {
            controller.abort();
            return {
              ...makeRes(req),
              message: {
                [Symbol.asyncIterator]: () => ({
                  next: () => Promise.resolve({ done: true, value: undefined }),
                  return: returned,
                }),
              },
            };
          },
        }),
      ).toBeRejectedWith(jasmine.objectContaining({ code: Code.Canceled }));
      expect(finished).toHaveBeenCalledOnceWith(Code.Canceled);
      expect(returned).toHaveBeenCalledTimes(1);
      expect(cleared).toHaveBeenCalledTimes(1);
    });

    it("reports cancellation to interceptors when the transport loses the abort reason", async () => {
      const controller = new AbortController();
      const failed = jasmine.createSpy("failed");
      await expectAsync(
        runStreamingCall({
          req: makeReq(),
          signal: controller.signal,
          interceptors: [
            (next) => async (req) => {
              try {
                return await next(req);
              } catch (e) {
                failed(ConnectError.from(e).code);
                throw e;
              }
            },
          ],
          async next() {
            controller.abort();
            throw new TypeError("fetch failed");
          },
        }),
      ).toBeRejectedWith(jasmine.objectContaining({ code: Code.Canceled }));
      expect(failed).toHaveBeenCalledOnceWith(Code.Canceled);
    });

    it("releases the request and deadline when an interceptor factory throws", async () => {
      const req = makeReq();
      const returned = jasmine
        .createSpy("returned")
        .and.resolveTo({ done: true });
      req.message = {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.resolve({ done: true, value: undefined }),
          return: returned,
        }),
      };
      const cleared = spyOn(globalThis, "clearTimeout").and.callThrough();
      const error = new ConnectError("interceptor failed", Code.Internal);
      await expectAsync(
        runStreamingCall({
          req,
          timeoutMs: 100,
          interceptors: [
            () => {
              throw error;
            },
          ],
          async next(request) {
            return makeRes(request);
          },
        }),
      ).toBeRejectedWith(error);
      expect(returned).toHaveBeenCalledTimes(1);
      expect(cleared).toHaveBeenCalledTimes(1);
    });

    it("does not replace cancellation with a response cleanup error", async () => {
      const controller = new AbortController();
      const finished = jasmine.createSpy("finished");
      const returned = jasmine
        .createSpy("returned")
        .and.rejectWith(new Error("cleanup failed"));
      const res = await runStreamingCall({
        req: makeReq(),
        signal: controller.signal,
        interceptors: [trace(finished)],
        async next(req) {
          return {
            ...makeRes(req),
            message: {
              [Symbol.asyncIterator]: () => ({
                next: () =>
                  Promise.resolve({
                    done: false,
                    value: create(StringValueSchema),
                  }),
                return: returned,
              }),
            },
          };
        },
      });
      const it = res.message[Symbol.asyncIterator]();
      await it.next();
      controller.abort();
      await it.return?.();
      await expectAsync(it.next()).toBeRejectedWith(
        jasmine.objectContaining({ code: Code.Canceled }),
      );
      expect(finished).toHaveBeenCalledOnceWith(Code.Canceled);
      expect(returned).toHaveBeenCalledTimes(1);
    });

    it("does not start a call through an unused lazy iterable", async () => {
      const started = jasmine.createSpy("started");
      async function* stream() {
        const res = await runStreamingCall({
          req: makeReq(),
          async next(req) {
            started();
            return makeRes(req);
          },
        });
        yield* res.message;
      }
      await stream().return();
      expect(started).not.toHaveBeenCalled();
    });
  });
});
