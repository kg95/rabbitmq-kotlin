package connection

class DefaultConnectionFactory(
    rabbitMQConnectionFactory: com.rabbitmq.client.ConnectionFactory = com.rabbitmq.client.ConnectionFactory()
): ConnectionFactory(rabbitMQConnectionFactory)
