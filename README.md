# rabbitmq-kotlin

This project adds simple ways of working with the popular message broker rabbitmq in kotlin.

## rabbitmq-kotlin-lib

This module is a library to work with rabbitmq in kotlin, by wrapping the 
[rabbitmq java client api](https://www.rabbitmq.com/api-guide.html).

### Features
* simple creation of typesafe rabbitmq consumer and producer objects
* simple declarative configuration of said objects via builder pattern
* response based instead of exception based return pattern
* jackson serialization type conversion
* customizable converter interface
* compatability with the [kotlinx.coroutines](https://github.com/Kotlin/kotlinx.coroutines)
framework

### Usage

#### Builder

```kotlin
val builder = RabbitMqBuilder(
    rabbitmqAccess = RabbitMqAccess("username", "password", "host", port),
    converter = JacksonConverter()
)
```

The `RabbitMqAccess` is used to connect to a running rabbitmq node, using the
required properties `username`, `password`, `host` and `port`. If for some reason
you want to connect to two different rabbitmq instances you will need two
different builder objects.

The converter property is used to convert messages of any type to a rabbitmq message.
The library provides an interface `Converter` along with two implementations of said
interface(`DefaultConverter` and `JacksonConverter`). If you want to use custom message 
serialization, you can create your own converter implementation.

The created `RabbitMqBuilder` object is used to instantiate both consumer and producer.

#### Response

Most function calls of the consumer and producer objects returns a `Response` object, which
is an instance of a sealed class, indicating success or failure of the called function.
Successfully executed methods will return an instance of the `Response.Success` class, which
holds a property `value` storing the desired return value. Whereas an instance of 
`Response.Failure` holds a property `error`, with detailed information about what went wrong. 

#### Consumer

```kotlin
val consumer = builder.consumer(
    virtualHost = "/",
    queueName = "queueName",
    type = UUID::class.java
)

runBlocking {
    val response = consumer.collectNextMessages(
        timeoutMillis = 1000,
        limit = 10
    )
    when(response) {
        is Response.Success -> {
            val messages: List<PendingRabbitMqMessage<UUID>> = it.value
            //do something with messages
            ...
            consumer.ackMessages(successfullyProcessedMessages)
            consumer.nackMessages(unsuccessfullyProcessedMessages)
        }
        is Response.Failure -> handleError(it.error)
    }
}
```

The builder is used to instantiate both consumer and producer, using the parameters 
`virtualHost`, `queueName` and `type`. The `type` property binds a consumer or producer 
to a certain type, enforcing type safety on all of its function calls.

The function `collectNextMessages()` is the main purpose of the consumer. It transforms the
asynchronous subscribe pattern into a synchronous callable function, returning up to `limit`
messages after a maximum of `timeoutMillis` milliseconds. As mentioned in the section above, it
returns a `Response` object, that if the operation was successful, holds a list of `PendingRabbitMqMessages`
in its property `value` or alternatively holds an error.

The type `PendingRabbitMqMessage` represents a message, currently in the queue. It holds the converted 
message in its value property, as well as necessary data to acknowledge the message later. 

The messages can then be used in some business logic.

Finally, the functions `ackMessages()` and `nackMessages()` are called. `ackMessages()` is used to
remove messages completely from the queue. This is usually the case when a message was 
successfully consumed. The `nackMessages()` function is used to requeue messages in the queue,
which is usually a good idea when your business logic fails while processing a message.
This way the message will not be lost, giving you time to fix the logic.

**Warning**: If nether of the two functions is called on a message, the message will be stuck
in the delivery state until either the connection to the message broker is lost/closed or 
the application shuts down, in which case all messages currently in delivery will be requeued.

**Note**: Since a consumer receives messages asynchronously, if the channel used to connect to
the message broker crashes for some reason, the consumer will no longer be able to receive messages.
For this reason the consumer implements a watchdog in a separate coroutine, to ensures the health 
of the channel. If you want to destroy a consumer object without shutting down the 
application, make sure to call the `close()` function of said consumer, otherwise you will
create a memory leak. You can configure the `CoroutineContext` the watchdog runs in using the
property `dispatcher` in the `consumerOptions` property of the builder.

#### Producer

```kotlin
val producer = builder.producer(
    virtualHost = "/",
    queueName = "queueName",
    type = UUID::class.java
)

val messages: List<UUID> = someUUIDs()
runBlocking {
    val response = producer.sendMessages(messages)
    if(response is Response.Failure) {
        hadlePublishError(response.error)
    }
}
```

As you can see the producer is instantiated exactly like the consumer.
The function `sendMessages()` is used to send a collection of messages of the
producers configured type to the rabbitmq message broker. It also returns a `Response`
when called, which is mainly useful for error handling.

## rabbitmq-generator-plugin

This module is a gradle plugin to generate code of the rabbitmq-kotlin-lib.

### Features

* automatically generates rabbitmq-kotlin-lib source code
* simple declarative configuration in the gradle build script
* type safety of configured queues

### Usage

build.gradle.kts:
```gradle
plugins {
    id("io.github.kg95.rabbitmq-generator-plugin") version "0.1"
}

rabbitmqGeneratorConfig {
    outputFile = project.file("$buildDir/generated/rabbitmq/queueUtils.kt")
    builderConfig {
        username = "guest"
        password = "guest"
        host = "localhost"
        port = 5672
        customConverterClass = MyConverter::class.java
        consumerPrefetchCount = 1000
        consumerWatchDogIntervalMillis = 10000
        producerPublishAttemptCount = 1
        producerPublishAttemptDelayMillis = 1000
    }
    
    vhostConfig {
        vhost = "/"
        queueConfig("queueName1", Int::class.java)
        queueConfig {  //alternativ syntax to configure a queue
            name = "queueName2"
            type = String::class.java
        }
    }
}
```

**rabbitmqGeneratorConfig properties**
* `outputFile`: the task output(defaults to project.file("$buildDir/generated/rabbitmq"))
* `vhostConfig`: configures all queues of a virtual host from a rabbitmq messages broker
(can be called multiple times to configure multiple virtualhosts)
  * `vhost`: configures the virtual host for the queues configured on the `vhostConfig`
  * `queueConfig`: configures a queue to generate producer and consumer for
(can be called multiple times to configure multiple queues)
* `builderConfig`: configures the generated builder object
  * `username`: username to connect to the rabbitmq message broker
  * `password`: password to connect to the rabbitmq message broker
  * `host`: host the rabbitmq message broker runs on
  * `port`: port the rabbitmq message broker listens on
  * `customConverterClass`: optional configuration of the converter, defaults to null meaning
the builder will be configured with`JacksonConverter`
  * `consumerPrefetchCount`: sets the prefetch count of the created consumers
  * `consumerWatchDogIntervalMillis`: sets the watchdog interval of the created consumers
  * `producerPublishAttemptCount`: sets the number of publish attempts of the created producers
  * `producerPublishAttemptDelayMillis`: sets the delay between publish attempts of the created producers

The only mandatory configuration property is the vhostConfig. If left empty the task
will not generate any output. Configuration of the other properties is optional but
sometimes desirable.

Once configured, simply execute the `rabbitmqGenerate` task.

## rabbitmq-kotlin-lib-demo

Simple demo application to showcase the basic usage of the rabbitmq-kotlin-lib module.