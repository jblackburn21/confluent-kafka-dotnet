using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Util.Internal;

namespace ProviderLocationPublisher
{
    public class StartPublishingLocation
    {
        public static StartPublishingLocation Instance = new StartPublishingLocation();
    }

    public class StopPublishingLocation
    {
        public static StopPublishingLocation Instance = new StopPublishingLocation();
    }

    public class GetPublishedEventsCount
    {
        public static GetPublishedEventsCount Instance = new GetPublishedEventsCount();
    }
    
    public class ProviderCoordinator : ReceiveActor
    {
        private IActorRef[] _providers = new IActorRef[]{};
        
        public ProviderCoordinator(int poolSize, EventBusFactory eventBusFactory)
        {
            Receive<StartPublishingLocation>(cmd =>
            {
                _providers = Enumerable
                    .Range(0, poolSize)
                    .Select(i =>
                    {
                        var providerId = Guid.NewGuid();

                        return Context.ActorOf(Provider.Props(providerId, eventBusFactory), providerId.ToString());
                    })
                    .ToArray();
                
                Sender.Tell(new Status.Success(null));
            });

            ReceiveAsync<StopPublishingLocation>(async cmd =>
            {
                var tasks = _providers
                    .Select(a => a.Ask<int>(StopPublishingLocation.Instance))
                    .ToArray();

                await Task.WhenAll(tasks);

                Sender.Tell(tasks.Sum(t => t.Result));
            });
        }
        
        public static Props Props(int poolSize, EventBusFactory eventBusFactory) 
            => Akka.Actor.Props.Create(() => new ProviderCoordinator(poolSize, eventBusFactory));
    }
}