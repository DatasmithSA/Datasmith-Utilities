namespace RabbitMQ.Receiver
{
    #region Using Directives

    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using RabbitMQ.Client;
    using RabbitMQ.Client.Events;

    #endregion //Using Directives

    class Program
    {
        #region Fields

        private static ConnectionFactory _connectionFactory;
        private static IConnection _connection1;
        private static IModel _channel1;
        private static EventingBasicConsumer _consumer1;
        private static string _queue1 = "task_queue_1";

        private static IConnection _connection2;
        private static IModel _channel2;
        private static EventingBasicConsumer _consumer2;
        private static string _queue2 = "task_queue_2";

        #endregion //Fields

        #region Methods

        static void Main(string[] args)
        {
            try
            {
                //“Turning the heartbeat on also makes the server check to see if the connection is still up, which can be very important.
                //If a connection goes bad after a message has been picked up by the subscriber but before it’s been acknowledged, the server
                //just assumes that the client is taking a long time, and the message gets “stuck” on the dead connection until it gets closed.
                //With the heartbeat turned on, the server will recognize when the connection goes bad and close it, putting the message back
                //in the queue so another subscriber can handle it. Without the heartbeat, I’ve had to go in manually and close the connection
                //in the Rabbit management UI so that the stuck message can get passed to a subscriber.”
                _connectionFactory = new ConnectionFactory() { HostName = "localhost", RequestedHeartbeat = TimeSpan.FromSeconds(30)};
                _connectionFactory.UserName = "admin";
                _connectionFactory.Password = "27830529";
                _connectionFactory.VirtualHost = "/";
                _connectionFactory.HostName = "localhost";
                _connectionFactory.Port = AmqpTcpEndpoint.UseDefaultPort;

                Reconnect1(_queue1);
                Reconnect2(_queue2);

                Console.WriteLine("Press [enter] to exit.");
                Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
            finally
            {
                _connection1.ConnectionShutdown -= _connection1_ConnectionShutdown;
                _connection2.ConnectionShutdown -= _connection2_ConnectionShutdown;
                Cleanup1();
                Cleanup2();
            }
        }

        private static void ConnectQueue1(string queueName)
        {
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId}");
            _connection1 = _connectionFactory.CreateConnection();
            _connection1.ConnectionShutdown += _connection1_ConnectionShutdown;
            _channel1 = _connection1.CreateModel(); //A virtual connection inside a connection. When publishing or consuming messages from a queue - it's all done over a channel.
            //Setting durable to true means that the queue is persisted on message broker (RabbitMQ service) restarts.
            //Marking messages as persistent doesn't fully guarantee that a message won't be lost.Although
            //it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has
            //accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2) for every message
            //--it may be just saved to cache and not really written to the disk.The persistence guarantees aren't strong,
            //but it's more than enough for our simple task queue.If you need a stronger guarantee then you can use publisher confirms.
            _channel1.QueueDeclare(queueName, durable: true, false, false, arguments: null);

            //In order to change this behavior we can use the BasicQos method with the prefetchCount = 1 setting.
            //This tells RabbitMQ not to give more than one message to a worker at a time.
            //Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the
            //previous one. Instead, it will dispatch it to the next worker that is not still busy.
            _channel1.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            _consumer1 = new EventingBasicConsumer(_channel1);
            _consumer1.Received += Consumer_1_Received;
            _channel1.BasicConsume(queueName, autoAck: false, consumer: _consumer1);
        }

        private static void _connection1_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine("Connection 1 broke!");
            Reconnect1(_queue1);
        }

        private static void Reconnect1(string queueName)
        {
            Cleanup1();
            var manualResetEvent = new ManualResetEventSlim(false); // state is initially false
            while (!manualResetEvent.Wait(3000)) // loop until state is true, checking every 3s
            {
                try
                {
                    ConnectQueue1(queueName);
                    Console.WriteLine("Reconnected to queue 1!");
                    manualResetEvent.Set();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Reconnect failed to queue 1!");
                    Thread.Sleep(3000);
                }
            }
        }

        private static void ConnectQueue2(string queueName)
        {
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId}");
            _connection2 = _connectionFactory.CreateConnection();
            _connection2.ConnectionShutdown += _connection2_ConnectionShutdown;

            _channel2 = _connection2.CreateModel(); //A virtual connection inside a connection. When publishing or consuming messages from a queue - it's all done over a channel.
            //Setting durable to true means that the queue is persisted on message broker (RabbitMQ service) restarts.
            //Marking messages as persistent doesn't fully guarantee that a message won't be lost.Although
            //it tells RabbitMQ to save the message to disk, there is still a short time window when RabbitMQ has
            //accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do fsync(2) for every message
            //--it may be just saved to cache and not really written to the disk.The persistence guarantees aren't strong,
            //but it's more than enough for our simple task queue.If you need a stronger guarantee then you can use publisher confirms.
            _channel2.QueueDeclare(queueName, durable: true, false, false, arguments: null);

            //In order to change this behavior we can use the BasicQos method with the prefetchCount = 1 setting.
            //This tells RabbitMQ not to give more than one message to a worker at a time.
            //Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the
            //previous one. Instead, it will dispatch it to the next worker that is not still busy.
            _channel2.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            _consumer2 = new EventingBasicConsumer(_channel2);
            _consumer2.Received += Consumer_2_Received;
            _channel2.BasicConsume(queueName, autoAck: false, consumer: _consumer2);
        }

        private static void _connection2_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine("Connection 2 broke!");
            Reconnect2(_queue2);
        }

        private static void Reconnect2(string queueName)
        {
            Cleanup2();
            var manualResetEvent = new ManualResetEventSlim(false); // state is initially false
            while (!manualResetEvent.Wait(3000)) // loop until state is true, checking every 3s
            {
                try
                {
                    ConnectQueue2(queueName);
                    Console.WriteLine("Reconnected to queue 2!");
                    manualResetEvent.Set();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Reconnect failed to queue 2!");
                    Thread.Sleep(3000);
                }
            }
        }

        private static void Consumer_1_Received(object sender, BasicDeliverEventArgs e)
        {
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId}");
            byte[] body = e.Body.ToArray();
            var channel = ((EventingBasicConsumer)sender).Model;
            try
            {
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"[x] Received: {message}");

                int dots = message.Split('.').Length - 1;
                Thread.Sleep(1000 * dots);
                Console.WriteLine($"[x] Done");

                if (message.Contains(".0") || message.Contains(".2"))
                {
                    Console.WriteLine("Sending ACK");
                    // Note: it is possible to access the channel via ((EventingBasicConsumer)sender).
                    ///Acknowledgement must be sent on the same channel that received the delivery. Attempts to acknowledge using a different channel will result in a channel-level protocol exception.
                    channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                }
                else
                {
                    throw new Exception("Could not process message.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message} Republishing message.");
                //Republish the message so it goes to the back of the queue.
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true; //This does the same thing as setting the DeliveryMode to 2.
                channel.BasicPublish(exchange: string.Empty, routingKey: "task_queue_1", basicProperties: properties, body: body);
                channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false); //ACK it so that the original message gets removed from the queue.
                //Console.WriteLine("Sending NACK");
                //channel.BasicNack(e.DeliveryTag, multiple: false, requeue: true); //Reject the message and get the consumer to requeue it: https://www.rabbitmq.com/nack.html
            }
        }

        private static void Consumer_2_Received(object sender, BasicDeliverEventArgs e)
        {
            Console.WriteLine($"Thread: {Thread.CurrentThread.ManagedThreadId}");
            byte[] body = e.Body.ToArray();
            var channel = ((EventingBasicConsumer)sender).Model;
            try
            {
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"[x] Received: {message}");

                int dots = message.Split('.').Length - 1;
                Thread.Sleep(1000 * dots);
                Console.WriteLine($"[x] Done");

                if (message.Contains(".0") || message.Contains(".2"))
                {
                    Console.WriteLine("Sending ACK");
                    // Note: it is possible to access the channel via ((EventingBasicConsumer)sender).
                    ///Acknowledgement must be sent on the same channel that received the delivery. Attempts to acknowledge using a different channel will result in a channel-level protocol exception.
                    channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                }
                else
                {
                    throw new Exception("Could not process message.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message} Republishing message.");
                //Republish the message so it goes to the back of the queue.
                var properties = channel.CreateBasicProperties();
                properties.Persistent = true; //This does the same thing as setting the DeliveryMode to 2.
                channel.BasicPublish(exchange: string.Empty, routingKey: "task_queue_2", basicProperties: properties, body: body);
                channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false); //ACK it so that the original message gets removed from the queue.
                //Console.WriteLine("Sending NACK");
                //channel.BasicNack(e.DeliveryTag, multiple: false, requeue: true); //Reject the message and get the consumer to requeue it: https://www.rabbitmq.com/nack.html
            }
        }

        static void Cleanup1()
        {
            try
            {
                if (_channel1 != null && _channel1.IsOpen)
                {
                    _channel1.Close();
                    _channel1 = null;
                }

                if (_connection1 != null && _connection1.IsOpen)
                {
                    _connection1.Close();
                    _connection1 = null;
                }
            }
            catch (IOException ex)
            {
                // Close() may throw an IOException if connection
                // dies - but that's ok (handled by reconnect)
            }
        }

        static void Cleanup2()
        {
            try
            {
                if (_channel2 != null && _channel2.IsOpen)
                {
                    _channel2.Close();
                    _channel2 = null;
                }

                if (_connection2 != null && _connection2.IsOpen)
                {
                    _connection2.Close();
                    _connection2 = null;
                }
            }
            catch (IOException ex)
            {
                // Close() may throw an IOException if connection
                // dies - but that's ok (handled by reconnect)
            }
        }

        #endregion //Methods
    }
}
