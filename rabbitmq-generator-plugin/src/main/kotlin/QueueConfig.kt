package io.github.kg95.rabbitmq.generator

import java.io.Serializable

class QueueConfig: Serializable {
    var name: String = ""
    var type: Class<*> = String::class.java
}
