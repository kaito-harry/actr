import type {
  DescFile,
  DescMessage,
  DescMethod,
  DescService,
} from "@bufbuild/protobuf";
import {
  createEcmaScriptPlugin,
  runNodeJs,
  type Schema,
} from "@bufbuild/protoplugin";

type PluginParams = {
  localFiles: Set<string>;
  remoteFiles: Set<string>;
  remoteFileMapping: Map<string, string>;
};

type AnalyzedMethod = {
  service: DescService;
  method: DescMethod;
  requestCompanionName: string;
};

type TypeRef = {
  proto_type: string;
  type_name: string;
  proto_package: string;
  proto_file: string;
};

type MethodMetadata = {
  name: string;
  snake_name: string;
  route_key: string;
  input_ref: TypeRef;
  output_ref: TypeRef;
};

type RemoteServiceMetadata = {
  name: string;
  package: string;
  proto_file: string;
  actr_type: string;
  client_type: string;
  methods: MethodMetadata[];
};

type LocalServiceMetadata = {
  name: string;
  package: string;
  proto_file: string;
  handler_interface: string;
  workload_type: string;
  dispatcher_type: string;
  methods: MethodMetadata[];
};

const VERSION = "0.4.21";

const plugin = createEcmaScriptPlugin<PluginParams>({
  name: "protoc-gen-actrframework-typescript",
  version: VERSION,
  parseOptions: parseOptions,
  generateTs(schema) {
    generateTypeScript(schema);
  },
});

runNodeJs(plugin);

function generateTypeScript(schema: Schema<PluginParams>): void {
  const localServiceMetadata: LocalServiceMetadata[] = [];
  const remoteServiceMetadata: RemoteServiceMetadata[] = [];
  const fallbackTargetType = buildFallbackTargetType(
    schema.options.remoteFileMapping,
  );
  const requestOwners = new Map<string, string>();

  for (const file of schema.files) {
    if (file.services.length === 0) {
      continue;
    }

    const role = inferRole(file, schema.options);
    const analyzedMethods = analyzeFileMethods(file, role === "remote");

    if (role === "local") {
      localServiceMetadata.push(
        ...buildLocalServiceMetadata(file, analyzedMethods),
      );
      continue;
    }

    validateUniqueRequestBindings(requestOwners, analyzedMethods);
    generateClientFile(schema, file, analyzedMethods);

    const explicitTargetType = parseActrType(
      schema.options.remoteFileMapping.get(normalizeProtoFileKey(file.name)) ??
        "",
    );
    const targetType = explicitTargetType ?? fallbackTargetType;
    if (!targetType) {
      throw new Error(
        `No actr_type mapping found for remote file ${file.name}. ` +
          "Use RemoteFileMapping=remote/path.proto=manufacturer:Service:version.",
      );
    }

    remoteServiceMetadata.push(
      ...buildRemoteServiceMetadata(file, analyzedMethods, targetType),
    );
  }

  const metadataFile = schema.generateFile("actr-gen-meta.json");
  metadataFile.print(
    JSON.stringify(
      {
        plugin_version: VERSION,
        language: "typescript",
        local_services: localServiceMetadata,
        remote_services: remoteServiceMetadata,
      },
      null,
      2,
    ),
  );
}

function generateClientFile(
  schema: Schema<PluginParams>,
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
): void {
  const generated = schema.generateFile(
    `${normalizeProtoFileKey(file.name)}_client.ts`,
  );
  generated.preamble(file);
  const bufferSymbol = generated.import("Buffer", "node:buffer");

  const toBinary = generated.import("toBinary", "@bufbuild/protobuf");
  const fromBinary = generated.import("fromBinary", "@bufbuild/protobuf");
  const create = generated.import("create", "@bufbuild/protobuf");
  const messageInitShape = generated.import(
    "MessageInitShape",
    "@bufbuild/protobuf",
    true,
  );

  for (const entry of analyzedMethods) {
    const requestType = generated.importShape(entry.method.input);
    const requestSchema = generated.importSchema(entry.method.input);
    const responseType = generated.importShape(entry.method.output);
    const responseSchema = generated.importSchema(entry.method.output);

    generated.print(
      generated.export("const", entry.requestCompanionName),
      " = {",
    );
    generated.print(
      "  routeKey: ",
      generated.string(routeKeyForMethod(entry.method)),
      ",",
    );
    generated.print(
      "  encode(message: ",
      messageInitShape,
      "<typeof ",
      requestSchema,
      ">): ",
      bufferSymbol,
      " {",
    );
    generated.print(
      "    return ",
      bufferSymbol,
      ".from(",
      toBinary,
      "(",
      requestSchema,
      ", ",
      create,
      "(",
      requestSchema,
      ", message)));",
    );
    generated.print("  },");
    generated.print("  decode(bytes: Uint8Array): ", requestType, " {");
    generated.print("    return ", fromBinary, "(", requestSchema, ", bytes);");
    generated.print("  },");
    generated.print("  response: {");
    generated.print(
      "    encode(message: ",
      messageInitShape,
      "<typeof ",
      responseSchema,
      ">): ",
      bufferSymbol,
      " {",
    );
    generated.print(
      "      return ",
      bufferSymbol,
      ".from(",
      toBinary,
      "(",
      responseSchema,
      ", ",
      create,
      "(",
      responseSchema,
      ", message)));",
    );
    generated.print("    },");
    generated.print("    decode(bytes: Uint8Array): ", responseType, " {");
    generated.print(
      "      return ",
      fromBinary,
      "(",
      responseSchema,
      ", bytes);",
    );
    generated.print("    },");
    generated.print("  },");
    generated.print("} as const;");
    generated.print("");
  }
}

function buildFallbackTargetType(
  remoteFileMapping: Map<string, string>,
): { manufacturer: string; name: string; version: string } | null {
  const mappingTypes = Array.from(remoteFileMapping.values())
    .map(parseActrType)
    .filter(
      (
        value,
      ): value is { manufacturer: string; name: string; version: string } =>
        value !== null,
    );
  return mappingTypes.length === 1 ? mappingTypes[0] : null;
}

function parseOptions(
  rawOptions: { key: string; value: string }[],
): PluginParams {
  const localFiles = new Set<string>();
  const remoteFiles = new Set<string>();
  const remoteFileMapping = new Map<string, string>();

  for (const { key, value } of rawOptions) {
    switch (key) {
      case "LocalFiles":
        appendPathList(localFiles, value);
        break;
      case "RemoteFiles":
        appendPathList(remoteFiles, value);
        break;
      case "RemoteFileMapping":
        appendRemoteMapping(remoteFileMapping, value);
        break;
      default:
        throw new Error(`Unknown option '${key}'.`);
    }
  }

  for (const file of localFiles) {
    if (remoteFiles.has(file)) {
      throw new Error(
        `${file}: appears in both LocalFiles and RemoteFiles; a file must belong to exactly one side.`,
      );
    }
  }

  return {
    localFiles,
    remoteFiles,
    remoteFileMapping,
  };
}

function appendPathList(target: Set<string>, rawValue: string): void {
  for (const item of rawValue.split(":")) {
    const normalized = normalizeProtoFileKey(item.trim());
    if (normalized) {
      target.add(normalized);
    }
  }
}

function appendRemoteMapping(
  target: Map<string, string>,
  rawValue: string,
): void {
  for (const item of rawValue.split(";")) {
    const trimmed = item.trim();
    if (!trimmed) {
      continue;
    }
    const idx = trimmed.indexOf("=");
    if (idx <= 0 || idx === trimmed.length - 1) {
      throw new Error(`Invalid RemoteFileMapping entry: ${trimmed}`);
    }
    const file = normalizeProtoFileKey(trimmed.slice(0, idx));
    const actrType = trimmed.slice(idx + 1);
    target.set(file, actrType);
  }
}

function inferRole(file: DescFile, params: PluginParams): "local" | "remote" {
  const normalized = normalizeProtoFileKey(file.name);
  if (params.remoteFiles.has(normalized)) {
    return "remote";
  }
  if (params.localFiles.has(normalized)) {
    return "local";
  }
  return file.services.length > 0 ? "local" : "remote";
}

function analyzeFileMethods(
  file: DescFile,
  requireUniqueRequestCompanions: boolean,
): AnalyzedMethod[] {
  const analyzedMethods: AnalyzedMethod[] = [];
  const companionNames = new Set<string>();

  for (const service of file.services) {
    for (const method of service.methods) {
      if (method.methodKind !== "unary") {
        throw new Error(
          `${service.typeName}.${method.name}: only unary RPC methods are supported for TypeScript generation.`,
        );
      }

      const requestCompanionName = requestCompanionNameForMethod(method);
      if (
        requireUniqueRequestCompanions &&
        companionNames.has(requestCompanionName)
      ) {
        throw new Error(
          `${service.typeName}.${method.name}: request companion '${requestCompanionName}' is duplicated within ${file.name}. ` +
            "Each request type name must map to exactly one RPC in the generated file.",
        );
      }
      if (requireUniqueRequestCompanions) {
        companionNames.add(requestCompanionName);
      }

      analyzedMethods.push({
        service,
        method,
        requestCompanionName,
      });
    }
  }

  return analyzedMethods;
}

function buildLocalServiceMetadata(
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
): LocalServiceMetadata[] {
  return groupAnalyzedMethodsByService(analyzedMethods).map(
    ([service, methods]) => ({
      name: service.name,
      package: packageNameForService(service),
      proto_file: protoFilePath(file),
      handler_interface: `${service.name}Handler`,
      workload_type: `${service.name}Workload`,
      dispatcher_type: `${service.name}Dispatcher`,
      methods: methods.map((entry) => buildMethodMetadata(entry.method)),
    }),
  );
}

function buildRemoteServiceMetadata(
  file: DescFile,
  analyzedMethods: AnalyzedMethod[],
  targetType: { manufacturer: string; name: string; version: string },
): RemoteServiceMetadata[] {
  return groupAnalyzedMethodsByService(analyzedMethods).map(
    ([service, methods]) => ({
      name: service.name,
      package: packageNameForService(service),
      proto_file: protoFilePath(file),
      actr_type: `${targetType.manufacturer}:${targetType.name}:${targetType.version}`,
      client_type: `${service.name}Client`,
      methods: methods.map((entry) => buildMethodMetadata(entry.method)),
    }),
  );
}

function groupAnalyzedMethodsByService(
  analyzedMethods: AnalyzedMethod[],
): Array<[DescService, AnalyzedMethod[]]> {
  const grouped = new Map<DescService, AnalyzedMethod[]>();
  for (const entry of analyzedMethods) {
    const existing = grouped.get(entry.service);
    if (existing) {
      existing.push(entry);
    } else {
      grouped.set(entry.service, [entry]);
    }
  }
  return Array.from(grouped.entries());
}

function buildMethodMetadata(method: DescMethod): MethodMetadata {
  return {
    name: method.name,
    snake_name: toSnakeCase(method.name),
    route_key: routeKeyForMethod(method),
    input_ref: buildTypeRef(method.input),
    output_ref: buildTypeRef(method.output),
  };
}

function buildTypeRef(message: DescMessage): TypeRef {
  return {
    proto_type: message.typeName,
    type_name: relativeTypeName(message),
    proto_package: message.file.proto.package,
    proto_file: protoFilePath(message.file),
  };
}

function relativeTypeName(message: DescMessage): string {
  const packageName = message.file.proto.package;
  const packagePrefix = packageName ? `${packageName}.` : "";
  return message.typeName.startsWith(packagePrefix)
    ? message.typeName.slice(packagePrefix.length)
    : message.typeName;
}

function packageNameForService(service: DescService): string {
  const suffix = `.${service.name}`;
  if (service.typeName.endsWith(suffix)) {
    return service.typeName.slice(0, -suffix.length);
  }
  return "";
}

function validateUniqueRequestBindings(
  requestOwners: Map<string, string>,
  analyzedMethods: AnalyzedMethod[],
): void {
  for (const entry of analyzedMethods) {
    const requestTypeName = entry.method.input.typeName;
    const routeKey = routeKeyForMethod(entry.method);
    const owner = requestOwners.get(requestTypeName);
    if (owner && owner !== routeKey) {
      throw new Error(
        `${routeKey}: request type ${requestTypeName} is already bound to ${owner}. ` +
          "TypeScript strong-associated generation requires each request type to map to exactly one RPC.",
      );
    }
    requestOwners.set(requestTypeName, routeKey);
  }
}

function requestCompanionNameForMethod(method: DescMethod): string {
  return method.input.name;
}

function routeKeyForMethod(method: DescMethod): string {
  return `${method.parent.typeName}.${method.name}`;
}

function parseActrType(
  value: string,
): { manufacturer: string; name: string; version: string } | null {
  const parts = value.split(":");
  if (parts.length !== 3) {
    return null;
  }
  const [manufacturer, name, version] = parts;
  if (!manufacturer || !name || !version) {
    return null;
  }
  return {
    manufacturer,
    name,
    version,
  };
}

function normalizePath(value: string): string {
  let normalized = value.replaceAll("\\", "/");
  while (normalized.startsWith("./")) {
    normalized = normalized.slice(2);
  }
  return normalized;
}

function protoFilePath(file: DescFile): string {
  return normalizeProtoPath(file.name);
}

function normalizeProtoPath(value: string): string {
  const normalized = normalizePath(value);
  return normalized.endsWith(".proto") ? normalized : `${normalized}.proto`;
}

function normalizeProtoFileKey(value: string): string {
  return normalizeProtoPath(value).slice(0, -".proto".length);
}

function toSnakeCase(value: string): string {
  return value
    .replace(/(.)([A-Z][a-z]+)/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase();
}
