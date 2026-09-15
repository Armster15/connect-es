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

import * as http2 from "node:http2";
import { create, toBinary, toJsonString } from "@bufbuild/protobuf";
import type {
  DescMessage,
  DescMethod,
  DescMethodStreaming,
  DescMethodUnary,
  Message,
} from "@bufbuild/protobuf";
import { Code, ConnectError, createClient } from "@connectrpc/connect";
import type { MethodSerializationLookup } from "@connectrpc/connect";
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
        let requestBytes: Uint8Array<ArrayBuffer>;
        let responseBytes: Uint8Array<ArrayBuffer>;
        let factoryCalls: number;
        let serializeError: ConnectError | undefined;
        let parseError: ConnectError | undefined;
        beforeEach(() => {
          requestBytes = new Uint8Array([0xff, 0x00, 0x80]);
          responseBytes = new Uint8Array([0xfe, 0x81, 0xff, 0x00]);
          factoryCalls = 0;
          serializeError = undefined;
          parseError = undefined;
        });
        const server = useNodeServer(() =>
          http2.createServer((request, response) => {
            const chunks: Uint8Array[] = [];
            request.on("data", (chunk: Buffer) => chunks.push(chunk));
            request.on("end", () => {
              if (serializeError || requestBytes.byteLength > 32) {
                expect(Buffer.concat(chunks).byteLength).toBe(0);
                response.end();
                return;
              }
              const enveloped = protocol !== "Connect" || stream;
              expect(Buffer.concat(chunks)).toEqual(
                Buffer.from(
                  enveloped ? encodeEnvelope(0, requestBytes) : requestBytes,
                ),
              );
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
          }),
        );

        async function call(useCustomSerialization: boolean) {
          const options = {
            baseUrl: server.getUrl(),
            httpVersion: "2",
            idleConnectionTimeoutMs: 5,
            useBinaryFormat,
            readMaxBytes: 32,
            writeMaxBytes: 32,
            methodSerialization<I extends DescMessage, O extends DescMessage>(
              method: DescMethodUnary<I, O> | DescMethodStreaming<I, O>,
              defaults: MethodSerializationLookup<I, O>,
            ): MethodSerializationLookup<I, O> | undefined {
              factoryCalls++;
              expect<DescMethod>(method).toBe(
                stream
                  ? ElizaService.method.introduce
                  : ElizaService.method.say,
              );
              if (!useCustomSerialization) {
                return undefined;
              }
              return {
                getI(binary) {
                  expect(binary).toBe(useBinaryFormat);
                  return {
                    ...defaults.getI(binary),
                    serialize(message) {
                      if (serializeError) {
                        throw serializeError;
                      }
                      expect<Message>(message).toEqual(input);
                      return requestBytes;
                    },
                  };
                },
                getO(binary) {
                  expect(binary).toBe(useBinaryFormat);
                  return {
                    ...defaults.getO(binary),
                    parse(bytes) {
                      if (parseError) {
                        throw parseError;
                      }
                      expect(bytes).toEqual(responseBytes);
                      return defaults.getO(binary).parse(defaultResponseBytes);
                    },
                  };
                },
              };
            },
          } satisfies ConnectTransportOptions;
          const client = createClient(ElizaService, createTransport(options));
          if (stream) {
            const messages = [];
            for await (const message of client.introduce({ name: "request" })) {
              messages.push(message);
            }
            expect(messages).toEqual([
              create(IntroduceResponseSchema, { sentence: "response" }),
            ]);
          } else {
            expect(await client.say({ sentence: "request" })).toEqual(
              create(SayResponseSchema, { sentence: "response" }),
            );
          }
          expect(factoryCalls).toBe(1);
        }

        it("uses custom request and response bytes", async () => {
          await call(true);
        });

        it("uses the default codec when the factory returns undefined", async () => {
          requestBytes = defaultRequestBytes;
          responseBytes = defaultResponseBytes;
          await call(false);
        });

        it("propagates custom serialization errors", async () => {
          serializeError = new ConnectError("custom serialize", Code.Internal);
          await expectAsync(call(true)).toBeRejectedWith(serializeError);
        });

        it("propagates custom parsing errors", async () => {
          parseError = new ConnectError("custom parse", Code.InvalidArgument);
          await expectAsync(call(true)).toBeRejectedWith(parseError);
        });

        it("limits custom request bytes", async () => {
          requestBytes = new Uint8Array(33);
          await expectAsync(call(true)).toBeRejectedWith(
            jasmine.objectContaining({ code: Code.ResourceExhausted }),
          );
        });

        it("limits custom response bytes", async () => {
          responseBytes = new Uint8Array(33);
          await expectAsync(call(true)).toBeRejectedWith(
            jasmine.objectContaining({ code: Code.ResourceExhausted }),
          );
        });
      });
    }
  }
}
