using System;

namespace Orders.Messages
{
    public abstract class BaseOrderEvent
    {
        protected BaseOrderEvent(Guid orderId)
        {
            OrderId = orderId;
        }

        public Guid OrderId { get; }
    }

    public class OrderCreated : BaseOrderEvent
    {
        public OrderCreated(Guid orderId) : base(orderId)
        {
        }
    }

    public class OrderShipped : BaseOrderEvent
    {
        public OrderShipped(Guid orderId) : base(orderId)
        {
        }
    }

    public class OrderCancelled : BaseOrderEvent
    {
        public OrderCancelled(Guid orderId) : base(orderId)
        {
        }
    }
}
