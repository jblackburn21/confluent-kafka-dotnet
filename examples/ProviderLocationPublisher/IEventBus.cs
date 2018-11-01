using System.Threading.Tasks;

namespace ProviderLocationPublisher
{
    public interface IEventBus
    {
        Task Publish<TEvent>(IntegrationEvent<TEvent> @event);
    }
    
    public delegate IEventBus EventBusFactory();
}