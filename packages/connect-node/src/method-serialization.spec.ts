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

import { afterEach, beforeEach, describe, it } from "node:test";
import * as assert from "node:assert";
import * as http2 from "node:http2";
import type { Socket } from "node:net";
import { create, toBinary, toJsonString } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import { encodeEnvelope } from "@connectrpc/connect/protocol";
import {
  createConnectTransport,
  createGrpcTransport,
  createGrpcWebTransport,
} from "./index.js";
import type { ConnectTransportOptions } from "./index.js";
import { useNodeServer } from "./use-node-server-helper.spec.js";
import {
  ElizaService,
  IntroduceRequestSchema,
  IntroduceResponseSchema,
  SayRequestSchema,
  SayResponseSchema,
} from "./testdata/gen/connectrpc/eliza/v1/eliza_pb.js";

for (const [protocol, createTransport] of [
  ["Connect", createConnectTransport],
  ["gRPC", createGrpcTransport],
  ["gRPC-web", createGrpcWebTransport],
] as const) {
  for (const useBinaryFormat of [true, false]) {
    for (const stream of [false, true]) {
      describe(`${protocol} methodSerialization (${useBinaryFormat ? "binary" : "JSON"}, ${stream ? "stream" : "unary"})`, () => {
        const inputSchema = stream ? IntroduceRequestSchema : SayRequestSchema;
        const outputSchema = stream
          ? IntroduceResponseSchema
          : SayResponseSchema;
        const input = stream
          ? create(IntroduceRequestSchema, { name: "request" })
          : create(SayRequestSchema, { sentence: "request" });
        const output = create(outputSchema, { sentence: "response" });
        const defaultRequestBytes = useBinaryFormat
          ? toBinary(inputSchema, input)
          : new TextEncoder().encode(toJsonString(inputSchema, input));
        const defaultResponseBytes = useBinaryFormat
          ? toBinary(outputSchema, output)
          : new TextEncoder().encode(toJsonString(outputSchema, output));
        const enveloped = protocol !== "Connect" || stream;
        let requestBytes: Uint8Array<ArrayBuffer>;
        let responseBytes: Uint8Array<ArrayBuffer>;
        let requestChunks: Uint8Array[];
        let factoryCalls: number;
        let serializeError: ConnectError | undefined;
        let parseError: ConnectError | undefined;
        beforeEach(() => {
          requestBytes = new Uint8Array([0xff, 0x00, 0x80]);
          responseBytes = new Uint8Array([0xfe, 0x81, 0xff, 0x00]);
          requestChunks = [];
          factoryCalls = 0;
          serializeError = undefined;
          parseError = undefined;
        });
        const sockets = new Set<Socket>();
        // Close fixture sockets before useNodeServer waits for connections to drain.
        afterEach(() => {
          for (const socket of sockets) {
            socket.destroy();
          }
          sockets.clear();
        });
        const server = useNodeServer(() =>
          http2
            .createServer((request, response) => {
              request.on("data", (chunk: Buffer) => requestChunks.push(chunk));
              request.on("end", () => {
                if (serializeError || requestBytes.byteLength > 32) {
                  response.end();
                  return;
                }
                response.writeHead(200, {
                  "content-type": request.headers["content-type"],
                });
                response.write(
                  enveloped ? encodeEnvelope(0, responseBytes) : responseBytes,
                );
                if (protocol === "Connect" && stream) {
                  response.write(
                    encodeEnvelope(2, new TextEncoder().encode("{}")),
                  );
                } else if (protocol === "gRPC-web") {
                  response.write(
                    encodeEnvelope(
                      128,
                      new TextEncoder().encode("grpc-status: 0\r\n"),
                    ),
                  );
                } else if (protocol === "gRPC") {
                  response.addTrailers({ "grpc-status": "0" });
                }
                response.end();
              });
            })
            .on("connection", (socket) => {
              sockets.add(socket);
            }),
        );
        async function call(useCustomSerialization: boolean) {
          const options = {
            baseUrl: server.getUrl(),
            httpVersion: "2",
            idleConnectionTimeoutMs: 5,
            defaultTimeoutMs: 1_000,
            useBinaryFormat,
            readMaxBytes: 32,
            writeMaxBytes: 32,
            methodSerialization(method, defaults) {
              factoryCalls++;
              assert.strictEqual(
                method,
                stream
                  ? ElizaService.method.introduce
                  : ElizaService.method.say,
              );
              if (!useCustomSerialization) {
                return undefined;
              }
              return {
                getI(binary) {
                  assert.strictEqual(binary, useBinaryFormat);
                  return {
                    ...defaults.getI(binary),
                    serialize(message) {
                      if (serializeError) {
                        throw serializeError;
                      }
                      assert.deepStrictEqual(message, input);
                      return requestBytes;
                    },
                  };
                },
                getO(binary) {
                  assert.strictEqual(binary, useBinaryFormat);
                  return {
                    ...defaults.getO(binary),
                    parse(bytes) {
                      if (parseError) {
                        throw parseError;
                      }
                      assert.deepStrictEqual(bytes, responseBytes);
                      return defaults.getO(binary).parse(defaultResponseBytes);
                    },
                  };
                },
              };
            },
          } satisfies ConnectTransportOptions;
          try {
            const client = createClient(ElizaService, createTransport(options));
            if (stream) {
              const messages = [];
              for await (const message of client.introduce({
                name: "request",
              })) {
                messages.push(message);
              }
              assert.deepStrictEqual(messages, [output]);
            } else {
              assert.deepStrictEqual(
                await client.say({ sentence: "request" }),
                output,
              );
            }
          } finally {
            assert.strictEqual(factoryCalls, 1);
            const received = Buffer.concat(requestChunks);
            if (serializeError || requestBytes.byteLength > 32) {
              assert.strictEqual(received.byteLength, 0);
            } else {
              assert.deepStrictEqual(
                received,
                Buffer.from(
                  enveloped ? encodeEnvelope(0, requestBytes) : requestBytes,
                ),
              );
            }
          }
        }

        it(
          "uses custom request and response bytes",
          { timeout: 5_000 },
          async () => {
            await call(true);
          },
        );

        it(
          "uses the default codec when the factory returns undefined",
          { timeout: 5_000 },
          async () => {
            requestBytes = defaultRequestBytes;
            responseBytes = defaultResponseBytes;
            await call(false);
          },
        );

        it(
          "propagates custom serialization errors",
          { timeout: 5_000 },
          async () => {
            serializeError = new ConnectError(
              "custom serialize",
              Code.Internal,
            );
            await assert.rejects(call(true), (error) => {
              assert.strictEqual(error, serializeError);
              return true;
            });
          },
        );

        it("propagates custom parsing errors", { timeout: 5_000 }, async () => {
          parseError = new ConnectError("custom parse", Code.InvalidArgument);
          await assert.rejects(call(true), (error) => {
            assert.strictEqual(error, parseError);
            return true;
          });
        });

        it("limits custom request bytes", { timeout: 5_000 }, async () => {
          requestBytes = new Uint8Array(33);
          await assert.rejects(call(true), { code: Code.ResourceExhausted });
        });

        it("limits custom response bytes", { timeout: 5_000 }, async () => {
          responseBytes = new Uint8Array(33);
          await assert.rejects(call(true), { code: Code.ResourceExhausted });
        });
      });
    }
  }
}
