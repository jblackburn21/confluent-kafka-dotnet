// Copyright 2018 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using MessageTypes;
using LogMessage = MessageTypes.LogMessage;


namespace AvroBlogExample
{
    /// <summary>
    ///     Complete source for the examples programs presented in the blog post:
    ///     [insert blog URL here]
    /// </summary>
    class Program
    {
        private const string TopicName = "subject-name-test";
        
        static void ProduceGeneric(string bootstrapServers, string schemaRegistryUrl, string subjectNameStrategy)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var serdeProviderConfig = new AvroSerdeProviderConfig
            {
                SchemaRegistryUrl = schemaRegistryUrl,
                SchemaRegistrySubjectNameStrategy = subjectNameStrategy
            };
            
            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var producer = new Producer<Null, GenericRecord>(producerConfig, null, serdeProvider.GetSerializerGenerator<GenericRecord>()))
            {
                var logLevelSchema = (EnumSchema)Schema.Parse(
                    File.ReadAllText("LogLevel.asvc"));

                var logMessageSchema = (RecordSchema)Schema
                    .Parse(File.ReadAllText("LogMessage.V1.asvc")
                        .Replace(
                            "MessageTypes.LogLevel", 
                            File.ReadAllText("LogLevel.asvc")));

                var record = new GenericRecord(logMessageSchema);
                record.Add("IP", "127.0.0.1");
                record.Add("Message", "a test log message");
                record.Add("Severity", new GenericEnum(logLevelSchema, "Error"));
                producer.ProduceAsync(TopicName, new Message<Null, GenericRecord> { Value = record })
                    .ContinueWith(task => Console.WriteLine(
                        task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}"));

                producer.Flush(TimeSpan.FromSeconds(30));
            }
        }

        static void ProduceSpecific(string bootstrapServers, string schemaRegistryUrl, string subjectNameStrategy)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

            var serdeProviderConfig = new AvroSerdeProviderConfig
            {
                SchemaRegistryUrl = schemaRegistryUrl,
                SchemaRegistrySubjectNameStrategy = subjectNameStrategy
            };
            
            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var producer = new Producer<Null, MessageTypes.LogMessage>(producerConfig, null, serdeProvider.GetSerializerGenerator<MessageTypes.LogMessage>()))
            {
                producer.ProduceAsync(TopicName, 
                    new Message<Null, MessageTypes.LogMessage> 
                    {
                        Value = new MessageTypes.LogMessage()
                        {
                            IP = "5555",
                            Message = "Hello world",
                            Severity = LogLevel.Info,
                            Tags = new Dictionary<string, string>(
                                    new []
                                    {
                                        new KeyValuePair<string, string>("tag1", "val1"), 
                                    }
                                )
//                            Name = "Hello world"
                        }
                    });
                
                producer.Flush(TimeSpan.FromSeconds(30));
            }
        }

        static void ConsumeSpecific(string bootstrapServers, string schemaRegistryUrl, string subjectNameStrategy)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            var consumerConfig = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            var serdeProviderConfig = new AvroSerdeProviderConfig
            {
                SchemaRegistryUrl = schemaRegistryUrl,
                SchemaRegistrySubjectNameStrategy = subjectNameStrategy
            };
            
            using (var serdeProvider = new AvroSerdeProvider(serdeProviderConfig))
            using (var consumer = new Consumer<Null, GenericRecord>(consumerConfig, null, serdeProvider.GetDeserializerGenerator<GenericRecord>()))
            {
                consumer.Subscribe(TopicName);

                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);

                        var value = consumeResult.Value;

                        var type = Type.GetType(value.Schema.Fullname);
                        
//                        var jbTest = (JBTest)Activator.CreateInstance(type);
//                        
//                        foreach (var field in value.Schema.Fields)
//                        {
//                            jbTest.Put(field.Pos, value[field.Name]);    
//                        }                        
//                        
//                        Console.WriteLine($"Message: {jbTest.Name}");
                        
                        Console.WriteLine($"{consumeResult.Topic} Offset: {consumeResult.Offset.Value} Type: {type.FullName}");


                        consumer.Commit(consumeResult, cts.Token);

//                        Console.WriteLine($"{consumeResult.Message.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}: [{consumeResult.Value.Severity}] {consumeResult.Value.Message}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"an error occured: {e.Error.Reason}");
                    }
                }

                // commit final offsets and leave the group.
                consumer.Close();
            }
        }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <generic-produce|specific-produce|consume> <bootstrap-servers> <schema-registry-url> <subject-name-strategy>");

        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                PrintUsage();
                return;
            }

            var mode = args[0];
            var bootstrapServers = args[1];
            var schemaRegistryUrl = args[2];
            var subjectNameStrategy = args.Length == 4 ? args[3] : "topic_name_strategy";

            switch (mode)
            {
                case "generic-produce":
                    ProduceGeneric(bootstrapServers, schemaRegistryUrl, subjectNameStrategy);
                    break;
                case "specific-produce":
                    ProduceSpecific(bootstrapServers, schemaRegistryUrl, subjectNameStrategy);
                    break;
                case "consume":
                    ConsumeSpecific(bootstrapServers, schemaRegistryUrl, subjectNameStrategy);
                    break;
                default:
                    PrintUsage();
                    break;
            }
        }
    }
}
