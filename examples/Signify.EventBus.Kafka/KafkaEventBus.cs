using System;
using System.Threading.Tasks;
using Avro;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using ProviderLocationPublisher;

namespace Signify.EventBus.Kafka
{
//    public class LogMessage : SpecificFixed
//    {
//        public LogMessage(uint size) : base(size)
//        {
//        }
//
//        public override Schema Schema { get; }
//    }
    
    public class KafkaEventBus : IEventBus
    {
        private readonly string _topic;
        private readonly IProducer<string, string> _producer;

        public KafkaEventBus(string topic, IProducer<string, string> producer)
        {
//            var config = new ProducerConfig();
            
//            var producer2 = new Producer<string, LogMessage>(
//                config, Serializers.UTF8, new AvroSerializer<LogMessage>())
            
            
            
            _topic = topic;
            _producer = producer;
        }

        public async Task Publish<TEvent>(IntegrationEvent<TEvent> @event)
        {
            var json = JsonConvert.SerializeObject(@event.Event);
            
            var message = new Message<string,string>()
            {
                Key = @event.Key,
                Value = json
            };

            try
            {
                var result = await _producer.ProduceAsync(_topic, message);
            }
            catch (KafkaException e)
            {
                var error = e.Error;

            }
            
        }
    }
}