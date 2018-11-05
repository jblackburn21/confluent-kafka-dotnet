using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Serilog;
using Serilog.Formatting.Compact;

namespace ProviderLocationConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.File(new CompactJsonFormatter(), "log.clef")
                .MinimumLevel.Information()
                .CreateLogger();
            
            Log.Information($"Started consumer, Ctrl-C to stop consuming");

            var cts = new CancellationTokenSource();
            
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            var bootstrapServers = "devdocker02:31092";
            var topics = new[] {"provider-location"};
            
            RunConsume(bootstrapServers, topics, cts.Token);
        }
        
        public static void RunConsume(string brokerList, IEnumerable<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = "csharp-consumer",
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            const int commitPeriod = 5;

            using (var consumer = new Consumer<string, string>(config))
            {
                // Note: All event handlers are called on the main .Consume thread.
                
                // Raised when the consumer has been notified of a new assignment set.
                // You can use this event to perform actions such as retrieving offsets
                // from an external source / manually setting start offsets using
                // the Assign method. You can even call Assign with a different set of
                // partitions than those in the assignment. If you do not call Assign
                // in a handler of this event, the consumer will be automatically
                // assigned to the partitions of the assignment set and consumption
                // will start from last committed offsets or in accordance with
                // the auto.offset.reset configuration parameter for partitions where
                // there is no committed offset.
                consumer.OnPartitionsAssigned += (_, partitions)
                    => Log.Information($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");

                // Raised when the consumer's current assignment set has been revoked.
                consumer.OnPartitionsRevoked += (_, partitions)
                    => Log.Information($"Revoked partitions: [{string.Join(", ", partitions)}]");

                consumer.OnPartitionEOF += (_, tpo)
                    => Log.Information($"Reached end of topic {tpo.Topic} partition {tpo.Partition}, next message will be at offset {tpo.Offset}");

                consumer.OnError += (_, e)
                    => Log.Information($"Error: {e.Reason}");

                consumer.OnStatistics += (_, json)
                    => Log.Information($"Statistics: {json}");

                consumer.Subscribe(topics);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Log.Information($"Topic: {consumeResult.Topic} Partition: {consumeResult.Partition} Offset: {consumeResult.Offset} Key: {consumeResult.Key} Value: {consumeResult.Value}");

                        if (consumeResult.Offset % commitPeriod == 0)
                        {
                            // The Commit method sends a "commit offsets" request to the Kafka
                            // cluster and synchronously waits for the response. This is very
                            // slow compared to the rate at which the consumer is capable of
                            // consuming messages. A high performance application will typically
                            // commit offsets relatively infrequently and be designed handle
                            // duplicate messages in the event of failure.
                            var committedOffsets = consumer.Commit(consumeResult);
                            Log.Information($"Committed offset: {committedOffsets}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Log.Information($"Consume error: {e.Error}");
                    }
                }

                consumer.Close();
            }
        }
    }
}