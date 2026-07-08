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
                println("protoc-gen-actrframework-kotlin 0.4.13")
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
                println("    0.4.13")
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

    // Build fully-qualified message name -> declaring (package, proto file) map
    // from the full descriptor set so imported RPC message types resolve to
    // their real owner outer class instead of the current service's package.
    val typeOwner = mutableMapOf<String, TypeOwner>()
    for (file in request.protoFileList) {
        for (message in file.messageTypeList) {
            collectTypeOwners(file, message, emptyList(), typeOwner)
        }
    }

    // Process each file to generate
    for (fileName in request.fileToGenerateList) {
        val fileDescriptor = request.protoFileList.find { it.name == fileName } ?: continue

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
        }
    }

    return responseBuilder.build()
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

/** Parse parameters from protoc --actrframework-kotlin_opt */
fun parseParameters(paramStr: String): Map<String, String> {
    return paramStr.split(",").filter { it.contains("=") }.associate {
        val (key, value) = it.split("=", limit = 2)
        key.trim() to value.trim()
    }
}
