plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlinx.kover")
    id("maven-publish")
}

group =
    providers
        .gradleProperty("actrGroup")
        .orElse("io.actrium")
        .get()

version =
    providers
        .gradleProperty("actrVersion")
        .orElse("0.0.0-dev")
        .get()

val publishUrl =
    providers
        .gradleProperty("actrPublishUrl")
        .orElse("https://maven.pkg.github.com/actrium/actr-kotlin-package-sync")

val actrSourceRepository = providers.gradleProperty("actrSourceRepository")
val actrSourceTag = providers.gradleProperty("actrSourceTag")
val actrSourceSha = providers.gradleProperty("actrSourceSha")
val actrValidationStagingUrl = providers.gradleProperty("actrValidationStagingUrl")

val githubPackagesUsername =
    providers
        .gradleProperty("gpr.user")
        .orElse(providers.environmentVariable("GITHUB_ACTOR"))

val githubPackagesToken =
    providers
        .gradleProperty("gpr.key")
        .orElse(providers.environmentVariable("GITHUB_TOKEN"))

android {
    namespace = "io.actrium.actr"
    compileSdk = 34

    defaultConfig {
        minSdk = 26
        consumerProguardFiles("consumer-rules.pro")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    // Suppress lint errors for UniFFI generated code
    lint {
        abortOnError = false
        disable += listOf("NewApi")
    }
}

dependencies {
    // Kotlin standard library
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.9.22")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    // JNA for UniFFI bindings - use Android-compatible version
    implementation("net.java.dev.jna:jna:5.14.0@aar")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.0")
}

// For publishing to local maven
publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "actr"

            afterEvaluate {
                from(components["release"])
            }

            pom {
                name.set("actr")
                description.set("Kotlin/Android SDK for the Actrium framework")
                url.set("https://github.com/Actrium/actr-kotlin-package-sync")
                actrSourceRepository.orNull?.let { properties.put("actr.source.repository", it) }
                actrSourceTag.orNull?.let { properties.put("actr.source.tag", it) }
                actrSourceSha.orNull?.let { properties.put("actr.source.sha", it) }

                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
            }
        }
    }

    repositories {
        if (actrValidationStagingUrl.isPresent) {
            maven {
                name = "validationStaging"
                url = uri(actrValidationStagingUrl.get())
            }
        } else {
            maven {
                name = "GitHubPackages"
                url = uri(publishUrl.get())
                credentials {
                    username = githubPackagesUsername.orNull
                    password = githubPackagesToken.orNull
                }
            }
        }
    }
}
