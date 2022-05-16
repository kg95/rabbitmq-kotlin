import io.github.kg95.rabbitmq.lib.model.RabbitMqAccess

fun main() {
    DemoApplication(
        numberOfMessages = 100000,
        producerCount = 5,
        consumerCount = 1,
        rabbitMQAccess = RabbitMqAccess("testuser", "testuser", "localhost", 5672),
        virtualHost = "/test",
        queueName = "test"
    ).run()
}
