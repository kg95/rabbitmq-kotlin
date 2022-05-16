package io.github.kg95.rabbitmq.generator

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension

class GeneratorPlugin: Plugin<Project> {
    override fun apply(project: Project) {
        project.extensions.create(
            "rabbitmqGeneratorConfig",
            GeneratorExtension::class.java
        )
        project.afterEvaluate {
            val extension = project.extensions.findByType(GeneratorExtension::class.java)
                ?: GeneratorExtension(project)

            project.tasks.register("rabbitmqGenerate", GeneratorTask::class.java) { task ->
                task.outputFile = extension.outputFile
                task.builderConfig = extension.builderConfig
                task.vhostConfig = extension.vhostConfig
                task.packageName = extension.packageName
            }

            val mainSourceSet =
                it.extensions.getByType(JavaPluginExtension::class.java).sourceSets.getByName("main")
            mainSourceSet.java.srcDir(extension.outputFile.parentFile)
            it.dependencies.add(
                mainSourceSet.implementationConfigurationName,
                "io.github.kg95:rabbitmq-kotlin-lib:1.0"
            )
        }
    }
}