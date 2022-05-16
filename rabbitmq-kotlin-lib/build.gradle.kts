import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
}

repositories {
    mavenCentral()
}

dependencies {
    //kotlin
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.6.0")

    //rabbitmq java api
    implementation("com.rabbitmq:amqp-client:5.14.2")

    //kotlin coroutines
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")

    //jackson serialization
    api("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
    api("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")

    //Test
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.8.2")
    testImplementation("io.mockk:mockk:1.12.4")
    testImplementation("org.assertj:assertj-core:3.22.0")
    testImplementation("org.awaitility:awaitility-kotlin:4.2.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.6.1")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
