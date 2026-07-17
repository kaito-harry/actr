plugins {
    kotlin("jvm") version "1.9.22"
    application
    id("com.google.protobuf") version "0.9.4"
}

group = "io.actrium"

version = "0.4.21"

repositories { mavenCentral() }

dependencies {
    implementation("com.google.protobuf:protobuf-java:3.25.1")
    implementation("com.squareup:kotlinpoet:1.16.0")
    testImplementation(kotlin("test"))
}

application { mainClass.set("io.actrium.codegen.MainKt") }

tasks.test { useJUnitPlatform() }
protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.1"
    }
}

application {
    mainClass.set("io.actrium.codegen.MainKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "io.actrium.codegen.MainKt"
    }
    // Create fat jar with all dependencies
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Create a custom task to build the protoc plugin
tasks.register<Jar>("protocPluginJar") {
    archiveBaseName.set("protoc-gen-actrframework-kotlin")
    archiveVersion.set("")
    manifest {
        attributes["Main-Class"] = "io.actrium.codegen.MainKt"
    }
    from(sourceSets.main.get().output)
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    useJUnitPlatform()
}
