import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import test from "node:test";

const projectDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const pluginPath = join(
  projectDir,
  "scripts",
  "protoc-gen-actrframework-typescript",
);

function generate(protoSource, options) {
  const root = mkdtempSync(join(tmpdir(), "actr-typescript-plugin-"));
  const output = join(root, "generated");
  const protoPath = join(root, "service.proto");
  mkdirSync(output);
  writeFileSync(protoPath, protoSource);

  const result = spawnSync(
    "protoc",
    [
      `--proto_path=${root}`,
      `--plugin=protoc-gen-actrframework-typescript=${pluginPath}`,
      `--actrframework-typescript_opt=target=ts,${options}`,
      `--actrframework-typescript_out=${output}`,
      protoPath,
    ],
    { encoding: "utf8" },
  );

  return { output, result };
}

test("local services may reuse one request type across RPC methods", () => {
  const { result } = generate(
    `syntax = "proto3";
package local;

message SharedRequest {}
message FirstResponse {}
message SecondResponse {}

service LocalService {
  rpc First(SharedRequest) returns (FirstResponse);
  rpc Second(SharedRequest) returns (SecondResponse);
}
`,
    "LocalFiles=service.proto",
  );

  assert.equal(result.status, 0, result.stderr);
});

test("remote service metadata preserves the complete actor type", () => {
  const { output, result } = generate(
    `syntax = "proto3";
package remote;

message Request {}
message Response {}

service RemoteService {
  rpc Call(Request) returns (Response);
}
`,
    "RemoteFiles=service.proto,RemoteFileMapping=service.proto=acme:RemoteService:1.2.3",
  );

  assert.equal(result.status, 0, result.stderr);
  const metadata = JSON.parse(
    readFileSync(join(output, "actr-gen-meta.json"), "utf8"),
  );
  assert.equal(
    metadata.remote_services[0].actr_type,
    "acme:RemoteService:1.2.3",
  );
});

test("metadata uses canonical acronym method names and proto paths", () => {
  const { output, result } = generate(
    `syntax = "proto3";
package local;

message Request {}
message Response {}

service LocalService {
  rpc HTTPServer(Request) returns (Response);
}
`,
    "LocalFiles=./service",
  );

  assert.equal(result.status, 0, result.stderr);
  const metadata = JSON.parse(
    readFileSync(join(output, "actr-gen-meta.json"), "utf8"),
  );
  assert.equal(metadata.local_services[0].proto_file, "service.proto");
  assert.equal(metadata.local_services[0].methods[0].snake_name, "http_server");
});
