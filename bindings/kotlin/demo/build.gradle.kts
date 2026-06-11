plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("com.google.protobuf")
}

android {
    namespace = "io.actorrtc.demo"
    compileSdk = 34

    defaultConfig {
        applicationId = "io.actorrtc.demo"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "1.0.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro",
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions { jvmTarget = "17" }

    buildFeatures {
        viewBinding = true
        compose = true
    }

    composeOptions { kotlinCompilerExtensionVersion = "1.5.8" }
}

// Protobuf configuration for message generation
protobuf {
    protoc { artifact = "com.google.protobuf:protoc:3.25.1" }
    generateProtoTasks {
        all().forEach { task -> task.builtins { create("java") { option("lite") } } }
    }
}

// Copy proto files from assets/protos to proto source dirs for protobuf plugin
// Exclude duplicate directories that conflict (echo-real-server has same messages as echo-echo-server)
val copyMainProtos =
    tasks.register<Copy>("copyMainProtos") {
        from("src/main/assets/protos") {
            include("**/*.proto")
            exclude("**/echo-real-server/**")
        }
        into("src/main/proto")
    }

val copyTestProtos =
    tasks.register<Copy>("copyTestProtos") {
        from("src/main/assets/protos") {
            include("**/*.proto")
            exclude("**/echo-real-server/**")
        }
        from("src/androidTest/assets/protos") { include("**/*.proto") }
        into("src/androidTest/proto")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    }
afterEvaluate {
    tasks.matching { it.name.startsWith("generate") && it.name.contains("Proto") }.configureEach {
        dependsOn(copyMainProtos, copyTestProtos)
    }
}

dependencies {
    // actr-kotlin library
    implementation(project(":actr-kotlin"))

    // Protobuf runtime
    implementation("com.google.protobuf:protobuf-javalite:3.25.1")

    // Android core
    implementation("androidx.core:core-ktx:1.12.0")
    implementation("androidx.appcompat:appcompat:1.6.1")
    implementation("com.google.android.material:material:1.11.0")

    // Compose
    implementation(platform("androidx.compose:compose-bom:2024.01.00"))
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-graphics")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.activity:activity-compose:1.8.2")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.7.0")
    implementation("androidx.lifecycle:lifecycle-viewmodel-compose:2.7.0")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.0")

    // Testing
    testImplementation("junit:junit:4.13.2")
    androidTestImplementation("androidx.test.ext:junit:1.1.5")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.5.1")

    debugImplementation("androidx.compose.ui:ui-tooling")
    debugImplementation("androidx.compose.ui:ui-test-manifest")
}
