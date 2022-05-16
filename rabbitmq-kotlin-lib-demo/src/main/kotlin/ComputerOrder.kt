import java.util.UUID

data class ComputerOrder(
    val orderId: UUID = UUID.randomUUID(),
    val customer: Customer = Customer(),
    val orderStatus: OrderStatus = OrderStatus.ORDERED,
    val partIds: List<UUID> = listOf(UUID.randomUUID(), UUID.randomUUID())
)

data class Customer (
    val id: UUID = UUID.randomUUID(),
    val name: String = "customerName",
)

enum class OrderStatus {
    ORDERED, IN_ASSEMBLY, IN_DELIVERY, DELIVERED
}
