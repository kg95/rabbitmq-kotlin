# RabbitMQKotlinDemoApp

Simple demo application to test the functionality of the rabbitmq-kotlin-lib.
It creates multiple coroutines, which each create a RabbitMqProducer or
a RabbitMqConsumer, to simulate the communication between multiple applications.

Requires a specifically configured rabbitmq messages broker to run on the machine 
the application is executed on. For that reason the project includes a preconfigured 
dockerfile to easily configure and run the messages broker. Simply run the gradle tasks
`docker` followed by `dockerRun` (provided by the 
[gradle-docker](https://github.com/palantir/gradle-docker) plugin by palantir) to create
the docker image and run it.