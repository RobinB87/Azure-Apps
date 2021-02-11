using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ServiceBusApp
{
    class Program
    {
        private const string ConnectionString =
            "Endpoint=sb://servicebusrobineu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DlR/9l+hwwJmVVarJOH4exvh9/b1nzhufzIA73jK+oc=";
        private const string QueueName = "ServiceBusRobinQueue";

        // Singleton pattern
        static ServiceBusClient Client;
        static ServiceBusClient ClientGet
        {
            get
            {
                // create a Service Bus client 
                return Client ??= new ServiceBusClient(ConnectionString);
            }
        }

        static ServiceBusSender Sender;
        static ServiceBusSender SenderGet
        {
            get
            {
                // create a sender for the queue 
                return Sender ??= ClientGet.CreateSender(QueueName);
            }
        }

        static ServiceBusProcessor Processor;
        static ServiceBusProcessor ProcessorGet
        {
            get
            {
                // create a processor that we can use to process the messages
                return Processor ??= ClientGet.CreateProcessor(QueueName, new ServiceBusProcessorOptions());
            }
        }

        static async Task Main(string[] args)
        {
            // send a message to the queue
            await SendMessageAsync();

            // send a batch of messages to the queue
            await SendMessageBatchAsync();

            // receive message from the queue
            await ReceiveMessagesAsync();
        }

        static async Task SendMessageAsync()
        {
            // create a message that we can send
            var message = new ServiceBusMessage("Hello world!");

            // send the message
            await SenderGet.SendMessageAsync(message);
            Console.WriteLine($"Sent a single message to the queue: {QueueName}");
        }

        static Queue<ServiceBusMessage> CreateMessages()
        {
            // create a queue containing the messages and return it to the caller
            var messages = new Queue<ServiceBusMessage>();
            messages.Enqueue(new ServiceBusMessage("First message in the batch"));
            messages.Enqueue(new ServiceBusMessage("Second message in the batch"));
            messages.Enqueue(new ServiceBusMessage("Third message in the batch"));
            return messages;
        }

        static async Task SendMessageBatchAsync()
        {
            // get the messages to be sent to the Service Bus queue
            var messages = CreateMessages();

            // total number of messages to be sent to the Service Bus queue
            var messageCount = messages.Count;

            // while all messages are not sent to the Service Bus queue
            while (messages.Count > 0)
            {
                // start a new batch 
                using ServiceBusMessageBatch messageBatch = await SenderGet.CreateMessageBatchAsync();

                // add the first message to the batch
                if (messageBatch.TryAddMessage(messages.Peek()))
                {
                    // dequeue the message from the .NET queue once the message is added to the batch
                    messages.Dequeue();
                }
                else
                {
                    // if the first message can't fit, then it is too large for the batch
                    throw new Exception($"Message {messageCount - messages.Count} is too large and cannot be sent.");
                }

                // add as many messages as possible to the current batch
                while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                {
                    // dequeue the message from the .NET queue as it has been added to the batch
                    messages.Dequeue();
                }

                // now, send the batch
                await SenderGet.SendMessagesAsync(messageBatch);

                // if there are any remaining messages in the .NET queue, the while loop repeats 
                Console.WriteLine($"Sent a batch of {messageCount} messages to the topic: {QueueName}");
            }
        }

        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            var body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");

            // complete the message. messages is deleted from the queue. 
            await args.CompleteMessageAsync(args.Message);
        }

        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        static async Task ReceiveMessagesAsync()
        {
            // add handler to process messages
            ProcessorGet.ProcessMessageAsync += MessageHandler;

            // add handler to process any errors
            ProcessorGet.ProcessErrorAsync += ErrorHandler;

            // start processing 
            await ProcessorGet.StartProcessingAsync();

            Console.WriteLine("Wait for a minute and then press any key to end the processing");
            Console.ReadKey();

            // stop processing 
            Console.WriteLine("\nStopping the receiver...");
            await ProcessorGet.StopProcessingAsync();
            Console.WriteLine("Stopped receiving messages");
        }
    }
}