namespace ProviderLocationPublisher
{
    public class IntegrationEvent<TEvent>
    {
        public IntegrationEvent(string key, TEvent @event)
        {
            Key = key;
            Event = @event;
        }

        public string Key { get; }
        
        public TEvent Event { get; }
    }
}