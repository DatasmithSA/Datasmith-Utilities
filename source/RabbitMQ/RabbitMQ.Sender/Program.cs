namespace RabbitMQ.Sender
{
    #region Using Directives

    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using RabbitMQ.Client;

    #endregion //Using Directives

    class Program
    {
        #region Constants

        private const string QUEUE_1 = "task_queue_1";
        private const string QUEUE_2 = "task_queue_2";

        #endregion //Constants

        #region Methods

        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId}");
                var factory = new ConnectionFactory() { HostName = "localhost" };
                factory.UserName = "admin";
                factory.Password = "27830529";
                factory.VirtualHost = "/";
                factory.HostName = "localhost";
                factory.Port = AmqpTcpEndpoint.UseDefaultPort;

                //Send to queue 1
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel()) //A virtual connection inside a connection. When publishing or consuming messages from a queue - it's all done over a channel.
                    {
                        for (int i = 0; i < 5; i++)
                        {
                            string message = GetMessage(args, i, QUEUE_1);
                            var body = Encoding.UTF8.GetBytes(message);

                            //Setting durable to true means that the queue is persisted on message broker (RabbitMQ service) restarts.
                            //Marking messages as persistent doesn't fully guarantee that a message won't be lost.Although
                            //it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has
                            //accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2) for every message
                            //--it may be just saved to cache and not really written to the disk.The persistence guarantees aren't strong,
                            //but it's more than enough for our simple task queue.If you need a stronger guarantee then you can use publisher confirms.
                            channel.QueueDeclare(QUEUE_1, durable: true, false, false, arguments: null);

                            //In order to change this behavior we can use the BasicQos method with the prefetchCount = 1 setting.
                            //This tells RabbitMQ not to give more than one message to a worker at a time.
                            //Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the
                            //previous one. Instead, it will dispatch it to the next worker that is not still busy.
                            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                            var properties = channel.CreateBasicProperties();
                            //properties.DeliveryMode = 2; //This sends a message with delivery mode 2 (persistent). 1 = non-persistant.
                            properties.Persistent = true; //This does the same thing as setting the DeliveryMode to 2.
                                                          //properties.Expiration = "300000"; //Sets the expiration time in milliseconds on the message. 36000000 = 10 hours. 300000 = 5 minutes.

                            channel.BasicPublish(exchange: string.Empty, routingKey: QUEUE_1, basicProperties: properties, body: body);
                            Console.WriteLine($"[x] Sent: {message}");
                        }
                    }
                }

                //Send to queue 2
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel()) 
                    {
                        for (int i = 0; i < 5; i++)
                        {
                            string message = GetMessage(args, i, QUEUE_2);
                            var body = Encoding.UTF8.GetBytes(message);
                            channel.QueueDeclare(QUEUE_2, durable: true, false, false, arguments: null);
                            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                            var properties = channel.CreateBasicProperties();
                            properties.Persistent = true; //This does the same thing as setting the DeliveryMode to 2.
                            channel.BasicPublish(exchange: string.Empty, routingKey: QUEUE_2, basicProperties: properties, body: body);
                            Console.WriteLine($"[x] Sent: {message}");
                        }
                    }
                }
                Console.WriteLine("Press [enter] to exit.");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
        }

        private static string GetMessage(string[] args, int counter, string queueName)
        {
            return ((args.Length > 0) ? string.Join(" ", args) + "." + counter.ToString() + " " + queueName : "Hello World!" + "." + counter.ToString() + " " + queueName);
        }

        #endregion //Methods
    }
}
