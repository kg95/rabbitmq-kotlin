package io.github.kg95.rabbitmq.generator

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import java.io.File
import kotlin.reflect.KClass

abstract class GeneratorTask: DefaultTask() {

    @get:OutputFile
    abstract var outputFile: File

    @Input
    var builderConfig: BuilderConfig = BuilderConfig()

    @get:Input
    var vhostConfig: List<VirtualHostConfig> = emptyList()

    @get:Input
    @Optional
    var packageName: String? = null

    @TaskAction
    fun generate() {
        if(vhostConfig.isEmpty()) {
            return
        }
        initOutputFile()
        generatePackageName()
        generateImportDirectives()
        generateBuilder()
        vhostConfig.map {
            generateProducers(it.vhost, it.queueConfig)
            generateConsumers(it.vhost, it.queueConfig)
       }
    }

    private fun initOutputFile() {
        outputFile.parentFile.mkdirs()
        outputFile.writeText("")
    }

    private fun generatePackageName() {
        packageName?.let {
            outputFile.appendText("package $it\n")
            outputFile.appendText("\n")

        }
    }

    private fun generateImportDirectives() {
        val types = queueTypes(vhostConfig.flatMap { it.queueConfig })
        outputFile.appendText(
            """
                import io.github.kg95.rabbitmq.lib.RabbitMQBuilder
                import io.github.kg95.rabbitmq.lib.model.ConsumerOptions
                import io.github.kg95.rabbitmq.lib.model.ProducerOptions
                import io.github.kg95.rabbitmq.lib.model.RabbitMQAccess
                import kotlinx.coroutines.Dispatchers
                
            """.trimIndent()
        )
        outputFile.appendText("import ${converterPackage()}\n")
        types.map {
            outputFile.appendText("import ${it.qualifiedName}\n")
        }
        outputFile.appendText("\n")
    }

    private fun converterPackage(): String {
        return builderConfig.customConverterClass?.let {
            "${it.kotlin.qualifiedName}"
        } ?: "io.github.kg95.rabbitmq.lib.converter.JacksonConverter"
    }

    private fun queueTypes(
        queueConfig: List<QueueConfig>
    ): Set<KClass<*>> {
        return queueConfig.map { it.type.kotlin }.toSet()
    }

    private fun generateBuilder() {
        outputFile.appendText(
            "private val rabbitmqAccess = RabbitMQAccess(" +
                    "\"${builderConfig.username}\", \"${builderConfig.password}\", " +
                    "\"${builderConfig.host}\", ${builderConfig.port}" +
                    ")\n"
        )
        outputFile.appendText(
            "private val consumerOptions = ConsumerOptions(Dispatchers.Default, " +
                    "${builderConfig.consumerPrefetchCount}, " +
                    "${builderConfig.consumerWatchDogIntervalMillis}" +
                    ")\n"
        )
        outputFile.appendText(
            "private val producerOptions = ProducerOptions(" +
                    "${builderConfig.producerPublishAttemptCount}, " +
                    "${builderConfig.producerPublishAttemptDelayMillis}" +
                    ")\n"
        )
        outputFile.appendText(
            "private val builder = RabbitMQBuilder(" +
                    "rabbitmqAccess, ${converterClass()}(), consumerOptions, producerOptions" +
                    ")\n"
        )
        outputFile.appendText("\n")
    }

    private fun converterClass(): String {
        return builderConfig.customConverterClass?.let {
            "${it.kotlin.simpleName}"
        } ?: "JacksonConverter"
    }

    private fun generateProducers(virtualHost: String, config: List<QueueConfig>) {
        config.map {
            generateProducer(virtualHost, it.name, it.type)
        }
    }

    private fun generateProducer(virtualHost: String, name: String, type: Class<*>) {
        outputFile.appendText(
            "fun ${name}Producer() = builder.producer(" +
                    "\"$virtualHost\", \"$name\", ${type.kotlin.simpleName}::class.java" +
                    ")\n"
        )
    }

    private fun generateConsumers(virtualHost: String, config: List<QueueConfig>) {
        config.map {
            generateConsumer(virtualHost, it.name, it.type)
        }
    }

    private fun generateConsumer(virtualHost: String, name: String, type: Class<*>) {
        outputFile.appendText(
            "fun ${name}Consumer() = builder.consumer(" +
                    "\"$virtualHost\", \"$name\", ${type.kotlin.simpleName}::class.java" +
                    ")\n"
        )
    }
}