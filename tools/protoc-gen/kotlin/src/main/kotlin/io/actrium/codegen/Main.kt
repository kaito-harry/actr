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

import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse

fun main(args: Array<String>) {
    // Support --version and --help arguments
    if (args.isNotEmpty()) {
        when (args[0]) {
            "--version", "-V" -> {
                println("protoc-gen-actrframework-kotlin 0.4.12")
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
                println("    0.4.12")
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
                            protoFileName = protoFileName
                    )

            val generatedFile = generator.generate()
            responseBuilder.addFile(generatedFile)
        }
    }

    return responseBuilder.build()
}

/** Parse parameters from protoc --actrframework-kotlin_opt */
fun parseParameters(paramStr: String): Map<String, String> {
    return paramStr.split(",").filter { it.contains("=") }.associate {
        val (key, value) = it.split("=", limit = 2)
        key.trim() to value.trim()
    }
}
