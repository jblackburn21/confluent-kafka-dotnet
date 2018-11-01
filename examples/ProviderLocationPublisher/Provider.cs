using System;
using Akka.Actor;
using Akka.Event;

namespace ProviderLocationPublisher
{
    public class PublishLocation
    {
        public static PublishLocation Instance = new PublishLocation();
    }
    
    public class Provider : ReceiveActor
    {
        private Guid _providerId;
        private IEventBus _eventBus;
        
        private readonly ICancelable _cancelPublishing;
        private readonly ILoggingAdapter _logger;

        private int _eventsPublished = 0;
        
        public Provider(Guid providerId, EventBusFactory eventBusFactory)
        {
            _providerId = providerId;
            _eventBus = eventBusFactory();
            
            
            _cancelPublishing = new Cancelable(Context.System.Scheduler);
            
            _logger = Context.GetLogger();

            ReceiveAsync<PublishLocation>(msg =>
            {
                var now = DateTime.Now;

                var location = new Coordinates(now.Ticks % 1000, now.Ticks % 500);
                
                var @event = new ProviderLocationUpdated(_providerId, location, now);
                
                //_logger.Info("Published: {@Event}", @event);

                _eventsPublished++;
                
                return _eventBus.Publish(new IntegrationEvent<ProviderLocationUpdated>(_providerId.ToString(), @event));
            });

            Receive<StopPublishingLocation>(cmd =>
            {
                _cancelPublishing.Cancel(false);
                
                Context.Stop(Self);

                Sender.Tell(_eventsPublished);
            });
        }
        
        public static Props Props(Guid providerId, EventBusFactory eventBusFactory)
            => Akka.Actor.Props.Create(() => new Provider(providerId, eventBusFactory));

        protected override void PreStart()
        {
            // TODO: The delay should probably be moved out
            var delay = (Math.Abs(Self.Path.Name.GetHashCode()) % 10) * 100;
                
            _logger.Info($"Provider: {_providerId} delaying {delay}ms");
            
            Context.System.Scheduler.ScheduleTellRepeatedly(
                initialDelay: TimeSpan.FromMilliseconds(delay),
                interval: TimeSpan.FromMilliseconds(50),
                receiver: Self,
                message: PublishLocation.Instance,
                sender: Self,
                cancelable: _cancelPublishing
            );
        }
    }

    public class Coordinates
    {
        public Coordinates(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }

        public double Latitude { get; }
        
        public double Longitude { get; } 
    }

    public class ProviderLocationUpdated
    {
        public ProviderLocationUpdated(Guid providerId, Coordinates location, DateTime createdOn)
        {
            ProviderId = providerId;
            Location = location;
            CreatedOn = createdOn;
        }

        public Guid ProviderId { get; }
        
        public Coordinates Location { get; }
        
        public DateTime CreatedOn { get; }
    }
}