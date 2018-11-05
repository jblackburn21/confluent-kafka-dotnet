using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Confluent.Kafka;
using Serilog;
using Serilog.Formatting.Compact;
using Signify.EventBus.Kafka;

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
}