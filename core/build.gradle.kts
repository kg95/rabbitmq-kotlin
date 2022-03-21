import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
}

group = "me.kgaehlsd"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val testOutput: Configuration by configurations.creating
dependencies {
    implementation(kotlin("stdlib"))
    implementation("com.rabbitmq:amqp-client:5.14.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.2")
    implementation("org.springframework.amqp:spring-rabbit:2.4.2")
    implementation("javax.annotation:javax.annotation-api:1.3.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.1")
    
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("io.mockk:mockk:1.10.0")
    testImplementation("org.assertj:assertj-core:3.16.1")
    testImplementation("org.awaitility:awaitility-kotlin:4.0.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.4.2")

    testOutput(sourceSets["test"].output)

}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
