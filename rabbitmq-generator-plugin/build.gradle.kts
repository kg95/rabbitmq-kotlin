import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "io.github.kg95"
version = "0.1"

plugins {
    kotlin("jvm") version "1.4.21"
    id("java-gradle-plugin")
}

repositories {
    mavenCentral()
}

dependencies {
    //kotlin
    implementation(kotlin("stdlib"))

    //gradle
    implementation(gradleApi())
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

gradlePlugin {
    plugins {
        create("rabbitmq-generator-plugin") {
            group = "io.github.kg95"
            id = "io.github.kg95.rabbitmq-generator-plugin"
            implementationClass = "io.github.kg95.rabbitmq.generator.GeneratorPlugin"
        }
    }
}
