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
import type { MethodSerializationFactory, Serialization } from "../index.js";
import {
  createBinarySerialization,
  createJsonSerialization,
  createMethodSerializationLookup,
  getJsonOptions,
  limitSerialization,
} from "./serialization.js";
import { Code } from "../code.js";
import { ConnectError } from "../connect-error.js";
import {
  SourceContextSchema,
  StringValueSchema,
  UInt32ValueSchema,
} from "@bufbuild/protobuf/wkt";
import {
  clone,
  create,
  equals,
  fromBinary,
  toBinary,
} from "@bufbuild/protobuf";
import { createServiceDesc } from "../descriptor-helper.spec.js";

describe("createMethodSerializationLookup()", () => {
  const method = createServiceDesc({
    typeName: "TestService",
    method: {
      unary: {
        input: StringValueSchema,
        output: StringValueSchema,
        methodKind: "unary",
      },
    },
  }).method.unary;
  const message = create(StringValueSchema, { value: "a" });
  const jsonBytes = new TextEncoder().encode('"a"');
  const limits = { readMaxBytes: 3, writeMaxBytes: 3 };
  const jsonFactory: MethodSerializationFactory = (method) => ({
    getI: () => createJsonSerialization(method.input, undefined),
    getO: () => createJsonSerialization(method.output, undefined),
  });

  it("uses custom request and response codecs", () => {
    const lookup = createMethodSerializationLookup(
      method,
      undefined,
      undefined,
      limits,
      jsonFactory,
    );
    assert.deepStrictEqual(lookup.getI(true).serialize(message), jsonBytes);
    assert.deepStrictEqual(lookup.getO(true).parse(jsonBytes), message);
  });

  it("uses default binary and JSON codecs when the factory returns undefined", () => {
    const lookup = createMethodSerializationLookup(
      method,
      undefined,
      undefined,
      limits,
      () => undefined,
    );
    assert.deepStrictEqual(
      lookup.getI(true).serialize(message),
      toBinary(StringValueSchema, message),
    );
    assert.deepStrictEqual(lookup.getO(false).parse(jsonBytes), message);
  });

  it("enforces read and write limits on custom codecs", () => {
    const lookup = createMethodSerializationLookup(
      method,
      undefined,
      undefined,
      limits,
      jsonFactory,
    );
    assert.throws(
      () =>
        lookup.getI(true).serialize(create(StringValueSchema, { value: "ab" })),
      { code: Code.ResourceExhausted },
    );
    assert.throws(
      () => lookup.getO(true).parse(new TextEncoder().encode('"ab"')),
      { code: Code.ResourceExhausted },
    );
  });

  for (const returnDefaults of [false, true]) {
    it(`preserves codec identity when the factory returns ${returnDefaults ? "defaults" : "undefined"}`, () => {
      const serializers: unknown[] = [];
      let factoryCalls = 0;
      const lookup = createMethodSerializationLookup(
        method,
        undefined,
        undefined,
        limits,
        (factoryMethod, defaults) => {
          factoryCalls++;
          assert.strictEqual(factoryMethod, method);
          serializers.push(
            defaults.getI(true),
            defaults.getI(false),
            defaults.getO(true),
            defaults.getO(false),
          );
          return returnDefaults ? defaults : undefined;
        },
      );
      assert.strictEqual(factoryCalls, 1);
      assert.strictEqual(lookup.getI(true), serializers[0]);
      assert.strictEqual(lookup.getI(false), serializers[1]);
      assert.strictEqual(lookup.getO(true), serializers[2]);
      assert.strictEqual(lookup.getO(false), serializers[3]);
      assert.deepStrictEqual(
        lookup.getI(true).serialize(create(StringValueSchema)),
        new Uint8Array(),
      );
      assert.deepStrictEqual(lookup.getO(false).parse(jsonBytes), message);
    });
  }

  it("gives the factory defaults with the configured binary and JSON options", () => {
    const method = createServiceDesc({
      typeName: "OptionsService",
      method: {
        unary: {
          input: SourceContextSchema,
          output: SourceContextSchema,
          methodKind: "unary",
        },
      },
    }).method.unary;
    const message = create(SourceContextSchema, { fileName: "test.proto" });
    const binaryBytes = toBinary(SourceContextSchema, message);
    const unknownBytes = new Uint8Array([...binaryBytes, 0x10, 1]);
    const messageWithUnknownFields = fromBinary(
      SourceContextSchema,
      unknownBytes,
    );
    const jsonBytes = new TextEncoder().encode('{"file_name":"test.proto"}');
    const lookup = createMethodSerializationLookup(
      method,
      { readUnknownFields: false, writeUnknownFields: false },
      { ignoreUnknownFields: false, useProtoFieldName: true },
      { readMaxBytes: 64, writeMaxBytes: 64 },
      (_method, defaults) => defaults,
    );
    for (const serialization of [lookup.getI(true), lookup.getO(true)]) {
      assert.deepStrictEqual(serialization.parse(unknownBytes), message);
      assert.deepStrictEqual(
        serialization.serialize(messageWithUnknownFields),
        binaryBytes,
      );
    }
    for (const serialization of [lookup.getI(false), lookup.getO(false)]) {
      assert.deepStrictEqual(serialization.serialize(message), jsonBytes);
      assert.deepStrictEqual(serialization.parse(jsonBytes), message);
      assert.throws(
        () =>
          serialization.parse(new TextEncoder().encode('{"unknown":"field"}')),
        { code: Code.InvalidArgument },
      );
    }
  });

  for (const useBinaryFormat of [true, false]) {
    it(`accepts custom bytes at the limit and rejects oversized bytes before parsing (${useBinaryFormat ? "binary" : "JSON"})`, () => {
      let bytes = new Uint8Array([0xff, 0x00, 0x80]);
      let parsed = 0;
      const factory: MethodSerializationFactory = (method) => ({
        getI: () => ({
          serialize: () => bytes,
          parse: () => {
            parsed++;
            return create(method.input);
          },
        }),
        getO: () => ({
          serialize: () => bytes,
          parse: () => {
            parsed++;
            return create(method.output);
          },
        }),
      });
      const lookup = createMethodSerializationLookup(
        method,
        undefined,
        undefined,
        limits,
        factory,
      );
      for (const serialization of [
        lookup.getI(useBinaryFormat),
        lookup.getO(useBinaryFormat),
      ]) {
        assert.strictEqual(
          serialization.serialize(create(StringValueSchema)),
          bytes,
        );
        assert.deepStrictEqual(
          serialization.parse(bytes),
          create(StringValueSchema),
        );
      }
      bytes = new Uint8Array(4);
      for (const serialization of [
        lookup.getI(useBinaryFormat),
        lookup.getO(useBinaryFormat),
      ]) {
        assert.throws(
          () => serialization.serialize(create(StringValueSchema)),
          { code: Code.ResourceExhausted },
        );
        assert.throws(() => serialization.parse(bytes), {
          code: Code.ResourceExhausted,
        });
      }
      assert.strictEqual(parsed, 2);
    });
  }
});

describe("createBinarySerialization()", () => {
  const goldenMessage = create(StringValueSchema, { value: "abc" });
  const goldenBytes = toBinary(
    StringValueSchema,
    create(StringValueSchema, { value: "abc" }),
  );
  const ser = createBinarySerialization(StringValueSchema, undefined);

  it("should serialize", () => {
    const bytes = ser.serialize(goldenMessage);
    assert.deepStrictEqual(bytes, goldenBytes);
  });

  it("should parse", () => {
    const message = ser.parse(goldenBytes);
    assert.ok(equals(StringValueSchema, goldenMessage, message));
  });

  describe("parsing invalid data", () => {
    it("should raise connect error", () => {
      assert.throws(
        () => ser.parse(new Uint8Array([0xde])),
        (e) => {
          assert.ok(e instanceof ConnectError);
          assert.strictEqual(
            e.message,
            "[internal] parse binary: premature EOF",
          );
          return true;
        },
      );
    });
  });

  describe("serializing invalid data", () => {
    it("should raise connect error", () => {
      const ser = createBinarySerialization(UInt32ValueSchema, undefined);
      const f = create(UInt32ValueSchema, { value: -1 });
      assert.throws(
        () => ser.serialize(f),
        (e) => {
          assert.ok(e instanceof ConnectError);
          assert.strictEqual(
            e.message,
            "[internal] serialize binary: cannot encode field google.protobuf.UInt32Value.value to binary: invalid uint32: -1",
          );
          return true;
        },
      );
    });
  });
});

describe("createJsonSerialization()", () => {
  const goldenMessage = create(StringValueSchema, { value: "abc" });
  const goldenBytes = new TextEncoder().encode(`"abc"`);
  const ser = createJsonSerialization(StringValueSchema, undefined);

  it("should serialize", () => {
    const bytes = ser.serialize(goldenMessage);
    assert.deepStrictEqual(bytes, goldenBytes);
  });

  it("should parse", () => {
    const message = ser.parse(goldenBytes);
    assert.ok(equals(StringValueSchema, goldenMessage, message));
  });

  describe("parsing invalid data", () => {
    it("should raise connect error", () => {
      assert.throws(
        () => ser.parse(new Uint8Array([0xde])),
        (e) => {
          assert.ok(e instanceof ConnectError);
          assert.match(
            e.message,
            /^\[invalid_argument] cannot decode message google.protobuf.StringValue from JSON: Unexpected token/,
          );
          return true;
        },
      );
    });
  });

  describe("serializing invalid data", () => {
    it("should raise connect error", () => {
      const f = clone(StringValueSchema, goldenMessage);
      f.value = new Error() as unknown as string;
      assert.throws(
        () => ser.serialize(f),
        (e) => {
          assert.ok(e instanceof ConnectError);
          assert.strictEqual(
            e.message,
            "[internal] cannot encode field google.protobuf.StringValue.value to JSON: expected string, got object",
          );
          return true;
        },
      );
    });
  });
});

describe("limitSerialization()", () => {
  const ser: Serialization<string> = {
    serialize(data: string): Uint8Array<ArrayBuffer> {
      return new TextEncoder().encode(data);
    },
    parse(data: Uint8Array): string {
      return new TextDecoder().decode(data);
    },
  };
  it("limits serialize", () => {
    const limitedSer = limitSerialization(ser, {
      readMaxBytes: 0xffffffff,
      writeMaxBytes: 3,
    });
    assert.throws(
      () => limitedSer.serialize("abcdef"),
      (err) =>
        err instanceof ConnectError &&
        err.message ===
          "[resource_exhausted] message size 6 is larger than configured writeMaxBytes 3",
    );
    assert.doesNotThrow(() =>
      limitedSer.parse(new TextEncoder().encode("abcdef")),
    );
  });
  it("limits parse", () => {
    const limitedSer = limitSerialization(ser, {
      readMaxBytes: 3,
      writeMaxBytes: 0xffffffff,
    });
    assert.doesNotThrow(() => limitedSer.serialize("abcdef"));
    assert.throws(
      () => limitedSer.parse(new TextEncoder().encode("abcdef")),
      (err) =>
        err instanceof ConnectError &&
        err.message ===
          "[resource_exhausted] message size 6 is larger than configured readMaxBytes 3",
    );
  });
});

describe("getJsonOptions()", () => {
  it("sets ignoreUnknownFields to true if not already set on options object", () => {
    const opts = getJsonOptions({ alwaysEmitImplicit: true });
    assert.strictEqual(opts.ignoreUnknownFields, true);
  });
  it("sets ignoreUnknownFields to true if undefined is passed", () => {
    const opts = getJsonOptions(undefined);
    assert.strictEqual(opts.ignoreUnknownFields, true);
  });
  it("doesn't change ignoreUnknownFields if already set", () => {
    const opts = getJsonOptions({ ignoreUnknownFields: false });
    assert.strictEqual(opts.ignoreUnknownFields, false);
  });
});
