package io.github.kg95.rabbitmq.generator

import org.gradle.api.Plugin
import org.gradle.api.Project

class GeneratorPlugin: Plugin<Project> {
    override fun apply(project: Project) {
        project.extensions.create(
            "rabbitmqGeneratorConfig",
            GeneratorExtension::class.java
        )

        project.afterEvaluate {
            val extension = project.extensions.findByType(GeneratorExtension::class.java)
                ?: GeneratorExtension(project)

            project.tasks.register("rabbitmqGenerate", GeneratorTask::class.java) {
                it.outputFile = extension.outputFile
                it.builderConfig = extension.builderConfig
                it.vhostConfig = extension.vhostConfig
                it.packageName = extension.packageName
            }
        }

    }
}