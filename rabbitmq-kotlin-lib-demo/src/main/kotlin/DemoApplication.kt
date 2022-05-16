import io.github.kg95.rabbitmq.lib.RabbitMqBuilder
import io.github.kg95.rabbitmq.lib.converter.JacksonConverter
import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess
import io.github.kg95.rabbitmq.lib.model.Response
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.random.Random

private const val CONSUMER_TIMEOUT = 1000L
private const val CHUNK_SIZE = 1000

class DemoApplication(
    private val numberOfMessages: Int = 100000,
    private val producerCount: Int = 1,
    private val consumerCount: Int = 1,
    val rabbitMQAccess: RabbitMqAccess = RabbitMqAccess(),
    private val virtualHost: String = "/",
    private val queueName: String = "/testQueue"
) {
    private val totalElementsToPublish: List<ComputerOrder> = generateComputerOrders()
    private var producersAreDone: Boolean = false
    private val totalReceivedElements: MutableList<ComputerOrder> = mutableListOf()

    private val rabbitmqBuilder = RabbitMqBuilder(
        rabbitMQAccess, JacksonConverter()
    )

    private fun generateComputerOrders(): List<ComputerOrder> {
        val list = mutableListOf<ComputerOrder>()
        for(i in 1..numberOfMessages) {
            list.add(ComputerOrder())
        }
        return list
    }

    fun run() {
        runBlocking {
            println("run: Launching producers and consumers")
            val producerJob = launch(Dispatchers.Default) { launchProducers() }
            val consumerJob = launch(Dispatchers.Default) { launchConsumers() }
            println("run: Producers and Consumers are running")
            producerJob.join()
            consumerJob.join()
        }
        println("run: All producers and consumers have finished. Evaluating experiment...")
        if(!experimentWasSuccessful()) {
            println("run: Failure! Received elements differ from the published!")
        } else {
            println("run: Success! Received elements are equal to the published!")
        }
    }

    private suspend fun launchProducers() {
        coroutineScope {
            println("launchProducers: launching $producerCount producers")
            val dividedOrderList = totalElementsToPublish.divide(producerCount)
            repeat(producerCount) {
                launch { producerTask(it, dividedOrderList[it]) }
            }
        }
        producersAreDone = true
        println("launchProducers: Producers are done!")
    }

    private suspend fun producerTask(producerNumber: Int, messages: List<ComputerOrder>) {
        val producer = rabbitmqBuilder.producer(virtualHost, queueName, ComputerOrder::class.java)
        println("producer $producerNumber: Sending messages")
        val messageChunks = messages.chunked(CHUNK_SIZE).toMutableList()
        while (messageChunks.isNotEmpty()) {
            val chunk = messageChunks.first()
            when(val response = producer.sendMessages(chunk)) {
                is Response.Failure -> {
                    println("producer $producerNumber: Producer encountered error while sending, ${response.error}")
                    continue
                }
                else -> {
                    messageChunks.remove(chunk)
                }
            }
        }
        producer.close()
        println("producer $producerNumber: Finished")
    }

    private suspend fun launchConsumers() {
        coroutineScope {
            println("launchConsumers: Launching $consumerCount consumers")
            val consumerList = mutableListOf<Deferred<List<ComputerOrder>>>()
            repeat(consumerCount) {
                consumerList.add(async { consumerTask(it) })
            }
            totalReceivedElements.addAll(consumerList.awaitAll().flatten())
        }
        println("launchConsumers: Consumers are done!")
    }

    private suspend fun consumerTask(consumerNumber: Int): List<ComputerOrder> {
        val receivedOrders = mutableListOf<ComputerOrder>()
        val consumer = rabbitmqBuilder.consumer(virtualHost, queueName, ComputerOrder::class.java)
        println("consumer $consumerNumber: Receiving messages")
        while (true) {
            when (val response = consumer.collectMessages(CONSUMER_TIMEOUT, CHUNK_SIZE)) {
                is Response.Success -> {
                    val rabbitmqMessages = response.value
                    when(processingWasSuccessful()) {
                        true -> {
                            receivedOrders.addAll(rabbitmqMessages.map { it.value })
                            consumer.ackMessages(rabbitmqMessages)
                        }
                        false -> {
                            println("consumer $consumerNumber: Failed to process message chunk")
                            consumer.nackMessages(rabbitmqMessages)
                        }
                    }
                    if (rabbitmqMessages.isEmpty() && producersAreDone) {
                        break
                    }
                }
                is Response.Failure -> {
                    println("consumer $consumerNumber: Encountered an error during receive, ${response.error}")
                    continue
                }
            }
        }
        consumer.close()
        println("consumer $consumerNumber: Finished")
        return receivedOrders
    }

    private fun processingWasSuccessful(): Boolean {
        return Random.nextInt(0, 100) < 90
    }

    private fun experimentWasSuccessful(): Boolean {
        val original = totalElementsToPublish.toHashSet()
        val received = totalReceivedElements.toHashSet()
        if(received.size != original.size) {
            return false
        }
        return received.containsAll(original)
    }
}

fun <T> List<T>.divide(count: Int): List<List<T>> {
    val chunkSize = if(size.mod(count) != 0) {
        size / count + 1
    } else {
        size / count
    }
    return chunked(chunkSize)
}
