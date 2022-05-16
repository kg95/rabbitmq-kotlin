import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    id("com.palantir.docker") version "0.33.0"
    id("com.palantir.docker-run") version "0.33.0"
    application
}

group = "me.kgaehlsd"
version = "1.0-SNAPSHOT"

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("io.github.kg95:rabbitmq-kotlin-lib:1.0")
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

docker {
    name = "demo-rabbitmq"
    setDockerfile(project.file("${projectDir}/src/docker/Dockerfile"))
    files(
        project.file("${projectDir}/src/docker/definition.json"),
        project.file("${projectDir}/src/docker/rabbitmq.conf")
    )
}

dockerRun {
    name = "demo-rabbitmq"
    image = "demo-rabbitmq:latest"
    ports("15672:15672","5672:5672")
}

application {
    mainClass.set("MainKt")
}