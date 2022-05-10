package io.github.kg95.rabbitmq.generator

import org.gradle.api.Project
import java.io.File
import javax.inject.Inject

open class GeneratorExtension @Inject constructor(project: Project) {
    var outputFile: File = project.file("${project.buildDir}/generated/rabbitmq/queueUtils.kt")
    var builderConfig: BuilderConfig = BuilderConfig()
    val vhostConfig: MutableList<VirtualHostConfig> = mutableListOf()
    var packageName: String? = null

    fun builderConfig(call: BuilderConfig.() -> Unit) {
        builderConfig = BuilderConfig().apply {
            call()
        }
    }

    fun vhostConfig(call: VirtualHostConfig.() -> Unit) {
        VirtualHostConfig().apply {
            call()
        }.let {
            vhostConfig.add(it)
        }
    }
}
