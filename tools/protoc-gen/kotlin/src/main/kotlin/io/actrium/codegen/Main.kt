/**
 * ACTR Framework Protoc Plugin for Kotlin
 *
 * This is a protoc plugin that generates Actor framework code from proto service definitions. It
 * generates:
 * - Handler interface for service implementations
 * - Dispatcher object for request routing
 *
 * Usage as protoc plugin: protoc --plugin=protoc-gen-actrframework-kotlin=PATH \
 * ```
 *          --actrframework-kotlin_out=OUT_DIR input.proto
 * ```
 */
package io.actrium.codegen

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse

fun main(args: Array<String>) {
    // Support --version and --help arguments
    if (args.isNotEmpty()) {
        when (args[0]) {
            "--version", "-V" -> {
                println("protoc-gen-actrframework-kotlin 0.5.0")
                return
            }
            "--help", "-h" -> {
                println(
                        "protoc-gen-actrframework-kotlin - Protobuf plugin for Actrium Kotlin framework"
                )
                println()
                println("USAGE:")
                println(
                        "    As protoc plugin: protoc --plugin=protoc-gen-actrframework-kotlin=PATH --actrframework-kotlin_out=OUT_DIR input.proto"
                )
                println("    Version info:     protoc-gen-actrframework-kotlin --version")
                println()
                println("VERSION:")
                println("    0.5.0")
                return
            }
        }
    }

    // Read CodeGeneratorRequest from stdin
    val request = CodeGeneratorRequest.parseFrom(System.`in`)

    // Generate code
    val response = generateCode(request)

    // Write CodeGeneratorResponse to stdout
    response.writeTo(System.out)
}

/** Generate code from the protoc request */
fun generateCode(request: CodeGeneratorRequest): CodeGeneratorResponse {
    val responseBuilder = CodeGeneratorResponse.newBuilder()

    // Parse parameters
    val params = parseParameters(request.parameter)
    val pluginParams = parsePluginParams(params)

    // Build fully-qualified message name -> declaring (package, proto file) map
    // from the full descriptor set so imported RPC message types resolve to
    // their real owner outer class instead of the current service's package.
    val typeOwner = mutableMapOf<String, TypeOwner>()
    for (file in request.protoFileList) {
        for (message in file.messageTypeList) {
            collectTypeOwners(file, message, emptyList(), typeOwner)
        }
    }

    val localServices = mutableListOf<ServiceMetadata>()
    val remoteServices = mutableListOf<ServiceMetadata>()

    // Process each file to generate
    for (fileName in request.fileToGenerateList) {
        val fileDescriptor = request.protoFileList.find { it.name == fileName } ?: continue
        val normalizedFileName = normalizeProtoPath(fileDescriptor.name)
        val role = pluginParams.roleFor(normalizedFileName)

        // Get proto file name (e.g., "file.proto" -> "file")
        val protoFileName = fileName.substringAfterLast("/")

        // Process each service in the file
        for (service in fileDescriptor.serviceList) {
            val generator =
                    KotlinActorGenerator(
                            packageName = fileDescriptor.`package`,
                            serviceName = service.name,
                            methods = service.methodList,
                            params = params,
                            protoFileName = protoFileName,
                            typeOwner = typeOwner
                    )

            val generatedFile = generator.generate()
            responseBuilder.addFile(generatedFile)

            val serviceMetadata =
                    ServiceMetadata(
                            name = service.name,
                            packageName = fileDescriptor.`package`,
                            protoFile = normalizedFileName,
                            methods =
                                    service.methodList.map { method ->
                                        MethodMetadata(
                                                name = method.name,
                                                snakeName = method.name.toSnakeCase(),
                                                routeKey =
                                                        routeKey(
                                                                fileDescriptor.`package`,
                                                                service.name,
                                                                method.name),
                                                inputRef =
                                                        buildTypeRef(
                                                                method.inputType,
                                                                "input",
                                                                service.name,
                                                                method.name,
                                                                normalizedFileName,
                                                                typeOwner,
                                                                generator.kotlinTypeName(
                                                                        method.inputType,
                                                                        "input",
                                                                        method.name)),
                                                outputRef =
                                                        buildTypeRef(
                                                                method.outputType,
                                                                "output",
                                                                service.name,
                                                                method.name,
                                                                normalizedFileName,
                                                                typeOwner,
                                                                generator.kotlinTypeName(
                                                                        method.outputType,
                                                                        "output",
                                                                        method.name)),
                                        )
                                    },
                    )
            if (role == FileRole.REMOTE) {
                remoteServices.add(serviceMetadata)
            } else {
                localServices.add(serviceMetadata)
            }
        }
    }

    responseBuilder.addFile(
            CodeGeneratorResponse.File.newBuilder()
                    .setName("actr-gen-meta.json")
                    .setContent(buildMetadataJson(localServices, remoteServices, pluginParams))
                    .build())

    return responseBuilder.build()
}

private enum class FileRole {
    LOCAL,
    REMOTE,
}

private data class PluginParams(
        val localFiles: Set<String>,
        val remoteFiles: Set<String>,
        val remoteFileMapping: Map<String, String>,
) {
    fun roleFor(fileName: String): FileRole {
        return when {
            remoteFiles.contains(fileName) -> FileRole.REMOTE
            localFiles.contains(fileName) -> FileRole.LOCAL
            fileName.startsWith("remote/") -> FileRole.REMOTE
            else -> FileRole.LOCAL
        }
    }
}

private data class TypeRefMetadata(
        val protoType: String,
        val typeName: String,
        val protoPackage: String,
        val protoFile: String,
        val generatedType: String,
)

private data class MethodMetadata(
        val name: String,
        val snakeName: String,
        val routeKey: String,
        val inputRef: TypeRefMetadata,
        val outputRef: TypeRefMetadata,
)

private data class ServiceMetadata(
        val name: String,
        val packageName: String,
        val protoFile: String,
        val methods: List<MethodMetadata>,
)

private fun parsePluginParams(params: Map<String, String>): PluginParams {
    return PluginParams(
            localFiles = parsePathSet(params["LocalFiles"]),
            remoteFiles = parsePathSet(params["RemoteFiles"]),
            remoteFileMapping =
                    parseMapping(params["RemoteFileMapping"] ?: params["RemoteFileActrTypes"]))
}

private fun parsePathSet(value: String?): Set<String> {
    if (value.isNullOrBlank()) return emptySet()
    return value.split(":").map { normalizeProtoPath(it.trim()) }.filter { it.isNotEmpty() }.toSet()
}

private fun parseMapping(value: String?): Map<String, String> {
    if (value.isNullOrBlank()) return emptyMap()
    return value.split(";")
            .mapNotNull { entry ->
                val trimmed = entry.trim()
                if (trimmed.isEmpty()) {
                    null
                } else {
                    val parts = trimmed.split("=", limit = 2)
                    require(parts.size == 2 && parts[0].isNotBlank() && parts[1].isNotBlank()) {
                        "Invalid RemoteFileMapping entry: $trimmed"
                    }
                    normalizeProtoPath(parts[0]) to parts[1]
                }
            }
            .toMap()
}

private fun buildTypeRef(
        rawType: String,
        kind: String,
        serviceName: String,
        methodName: String,
        protoFileName: String,
        typeOwner: Map<String, TypeOwner>,
        generatedType: String,
): TypeRefMetadata {
    val cleaned = rawType.trimStart('.')
    val owner =
            typeOwner[cleaned]
                    ?: throw IllegalArgumentException(
                            "Cannot resolve $kind type `$cleaned` for $serviceName.$methodName in " +
                                    "$protoFileName: RPC types must be declared in one of the parsed proto files")
    return TypeRefMetadata(
            protoType = cleaned,
            typeName = owner.messagePath.joinToString("."),
            protoPackage = owner.packageName,
            protoFile = normalizeProtoPath(owner.protoFileName),
            generatedType = generatedType,
    )
}

private fun buildMetadataJson(
        localServices: List<ServiceMetadata>,
        remoteServices: List<ServiceMetadata>,
        params: PluginParams,
): String {
    return buildString {
        append("{\n")
        append("  \"plugin_version\": \"0.4.13\",\n")
        append("  \"language\": \"kotlin\",\n")
        append("  \"local_services\": ")
        appendServiceArray(localServices, FileRole.LOCAL, params)
        append(",\n")
        append("  \"remote_services\": ")
        appendServiceArray(remoteServices, FileRole.REMOTE, params)
        append("\n")
        append("}\n")
    }
}

private fun StringBuilder.appendServiceArray(
        services: List<ServiceMetadata>,
        role: FileRole,
        params: PluginParams,
) {
    append("[")
    if (services.isNotEmpty()) append("\n")
    services.forEachIndexed { index, service ->
        append("    {\n")
        appendJsonField("name", service.name, indent = 6, trailing = true)
        appendJsonField("package", service.packageName, indent = 6, trailing = true)
        appendJsonField("proto_file", service.protoFile, indent = 6, trailing = true)
        if (role == FileRole.LOCAL) {
            appendJsonField("handler_interface", "${service.name}Handler", indent = 6, trailing = true)
            appendJsonField("workload_type", "${service.name}Workload", indent = 6, trailing = true)
            appendJsonField("dispatcher_type", "${service.name}Dispatcher", indent = 6, trailing = true)
        } else {
            val actrType =
                    params.remoteFileMapping[normalizeProtoPath(service.protoFile)]
                            ?: throw IllegalArgumentException(
                                    "No actr_type mapping found for remote file ${service.protoFile}")
            appendJsonField("actr_type", actrType, indent = 6, trailing = true)
            appendJsonField("client_type", "${service.name}Client", indent = 6, trailing = true)
        }
        append("      \"methods\": ")
        appendMethodArray(service.methods)
        append("\n")
        append("    }")
        if (index != services.lastIndex) append(",")
        append("\n")
    }
    append("  ]")
}

private fun StringBuilder.appendMethodArray(methods: List<MethodMetadata>) {
    append("[")
    if (methods.isNotEmpty()) append("\n")
    methods.forEachIndexed { index, method ->
        append("        {\n")
        appendJsonField("name", method.name, indent = 10, trailing = true)
        appendJsonField("snake_name", method.snakeName, indent = 10, trailing = true)
        appendJsonField("route_key", method.routeKey, indent = 10, trailing = true)
        append("          \"input_ref\": ")
        appendTypeRef(method.inputRef)
        append(",\n")
        append("          \"output_ref\": ")
        appendTypeRef(method.outputRef)
        append("\n")
        append("        }")
        if (index != methods.lastIndex) append(",")
        append("\n")
    }
    append("      ]")
}

private fun StringBuilder.appendTypeRef(ref: TypeRefMetadata) {
    append("{")
    append("\"proto_type\": ")
    appendJsonString(ref.protoType)
    append(", \"type_name\": ")
    appendJsonString(ref.typeName)
    append(", \"proto_package\": ")
    appendJsonString(ref.protoPackage)
    append(", \"proto_file\": ")
    appendJsonString(ref.protoFile)
    append(", \"generated_type\": ")
    appendJsonString(ref.generatedType)
    append("}")
}

private fun StringBuilder.appendJsonField(
        key: String,
        value: String,
        indent: Int,
        trailing: Boolean,
) {
    append(" ".repeat(indent))
    appendJsonString(key)
    append(": ")
    appendJsonString(value)
    if (trailing) append(",")
    append("\n")
}

private fun StringBuilder.appendJsonString(value: String) {
    append("\"")
    for (char in value) {
        when (char) {
            '\\' -> append("\\\\")
            '"' -> append("\\\"")
            '\n' -> append("\\n")
            '\r' -> append("\\r")
            '\t' -> append("\\t")
            else -> append(char)
        }
    }
    append("\"")
}

private fun routeKey(packageName: String, serviceName: String, methodName: String): String {
    return if (packageName.isBlank()) "$serviceName.$methodName"
    else "$packageName.$serviceName.$methodName"
}

private fun normalizeProtoPath(value: String): String {
    var normalized = value.replace("\\", "/")
    while (normalized.startsWith("./")) {
        normalized = normalized.removePrefix("./")
    }
    return if (normalized.endsWith(".proto")) normalized else "$normalized.proto"
}

private fun collectTypeOwners(
        file: FileDescriptorProto,
        message: DescriptorProto,
        parentPath: List<String>,
        owners: MutableMap<String, TypeOwner>,
) {
    val messagePath = parentPath + message.name
    val protoName = (listOf(file.`package`).filter { it.isNotEmpty() } + messagePath).joinToString(".")
    owners[protoName] =
            TypeOwner(
                    packageName = file.`package`,
                    protoFileName = file.name,
                    javaPackage = file.options.javaPackage,
                    javaOuterClassName =
                            file.options.javaOuterClassname.ifEmpty { defaultJavaOuterClassName(file) },
                    javaMultipleFiles = file.options.javaMultipleFiles,
                    messagePath = messagePath,
            )
    for (nested in message.nestedTypeList) {
        collectTypeOwners(file, nested, messagePath, owners)
    }
}

private fun defaultJavaOuterClassName(file: FileDescriptorProto): String {
    val baseName = file.name.substringAfterLast("/").removeSuffix(".proto").toPascalCase()
    val topLevelNames =
            file.messageTypeList.map { it.name }.toSet() +
                    file.enumTypeList.map { it.name } +
                    file.serviceList.map { it.name }
    return if (baseName in topLevelNames) "${baseName}OuterClass" else baseName
}

private fun String.toPascalCase(): String {
    return this.split("_", "-").joinToString("") { word ->
        word.replaceFirstChar { it.uppercase() }
    }
}

private fun String.toSnakeCase(): String {
    return this.replace(Regex("(.)([A-Z][a-z]+)"), "$1_$2")
            .replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
            .lowercase()
}

/** Parse parameters from protoc --actrframework-kotlin_opt */
fun parseParameters(paramStr: String): Map<String, String> {
    return paramStr.split(",").filter { it.contains("=") }.associate {
        val (key, value) = it.split("=", limit = 2)
        key.trim() to value.trim()
    }
}
