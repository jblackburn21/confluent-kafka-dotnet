using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Configuration;
using Akka.Event;
using Akka.Routing;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Newtonsoft.Json;
using Serilog;
using Serilog.Formatting.Compact;

namespace ProviderLocationPublisher
{
    class Program
    {
        private const int ProviderCount = 400;
        
        static async Task Main(string[] args)
        {
            Serilog.Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(new CompactJsonFormatter(), "log.clef")
                .MinimumLevel.Information()
                .CreateLogger();

            var hocon = await File.ReadAllTextAsync("hocon.conf");
            var config = ConfigurationFactory.ParseString(hocon);
            var actorSystem = ActorSystem.Create("ProviderLocation", config);


            IEventBus EventBusFactory()
            {
                var producerConfig = new ProducerConfig()
                {
                    BootstrapServers = "devdocker02:31092"
                };

                var producer = new Producer<string, string>(producerConfig);
                
                return new KafkaEventBus("provider-location", producer);
            }

            
            var providerCoordinator = actorSystem.ActorOf(ProviderCoordinator.Props(ProviderCount, EventBusFactory), "providers");
            
            Log.Information("Starting publishing...");
            
            var sw = new Stopwatch();
            sw.Start();
            
            await providerCoordinator.Ask<Status.Success>(StartPublishingLocation.Instance);
            
            Console.ReadLine();

            Log.Information("Stopping publishing...");
            
            var eventsPublished = await providerCoordinator.Ask<int>(StopPublishingLocation.Instance);
                        
            Log.Information("Started publishing");
            
            sw.Stop();    
            
            await providerCoordinator.GracefulStop(TimeSpan.FromSeconds(500));
            
            Log.Logger.Information($"Published {eventsPublished} events in {sw.ElapsedMilliseconds}ms");
        }
    }

    public class KafkaEventBus : IEventBus
    {
        private readonly string _topic;
        private readonly IProducer<string, string> _producer;

        public KafkaEventBus(string topic, IProducer<string, string> producer)
        {
            _topic = topic;
            _producer = producer;
        }

        public Task Publish<TEvent>(IntegrationEvent<TEvent> @event)
        {
            var json = JsonConvert.SerializeObject(@event.Event);
            
            var message = new Message<string,string>()
            {
                Key = @event.Key,
                Value = json
            };

            return _producer.ProduceAsync(_topic, message);
        }
    }
 }