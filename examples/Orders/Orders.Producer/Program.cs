using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Orders.Producer
{
    class Program
    {
        private const string TopicName = "orders";
        private const string SubjectNameStrategy = "topic_record_name_strategy";

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                PrintUsage();
                return;
            }

            var bootstrapServers = args[0];
            var schemaRegistryUrl = args[1];

            Produce(bootstrapServers, schemaRegistryUrl).GetAwaiter().GetResult();
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <bootstrap-servers> <schema-registry-url>");

        static async Task Produce(string bootstrapServers, string schemaRegistryUrl)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var serdeProviderConfig = new AvroSerdeProviderConfig
            {
                SchemaRegistryUrl = schemaRegistryUrl,
                SchemaRegistrySubjectNameStrategy = SubjectNameStrategy
            };

            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var producer = new Producer<Null, MessageTypes.LogMessage>(producerConfig, null, serdeProvider.GetSerializerGenerator<MessageTypes.LogMessage>()))
            {
                await producer.ProduceAsync(TopicName,
                    new Message<Null, MessageTypes.LogMessage>
                    {
                        Value = new MessageTypes.LogMessage
                        {
                            IP = "192.168.0.1",
                            Message = "a test message 2",
                            Severity = MessageTypes.LogLevel.Info,
                            Tags = new Dictionary<string, string> { { "location", "CA" } }
                        }
                    });
                //producer.Flush(TimeSpan.FromSeconds(30));
            }
        }
    }
}
