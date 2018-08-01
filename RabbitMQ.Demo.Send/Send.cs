using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace RabbitMQ.Demo.Send
{
    class Send
    {
        static void Main(string[] args)
        {
            #region Hello World 
            ////定义一个连接，指定要连接到哪里
            ////记着配置之前设置的用户名和密码
            //var factory = new ConnectionFactory()
            //{
            //    HostName = "localhost",
            //    UserName = "andy",
            //    Password = "youngdreamer"
            //};
            ////创建连接 
            //using (var connection = factory.CreateConnection())
            ////创建管道
            //using (var channel = connection.CreateModel())
            //{
            //    //定义一个队列
            //    channel.QueueDeclare(queue: "hello",
            //                        durable: false,//持久的
            //                        exclusive: false,//独有的，排斥
            //                        autoDelete: false,
            //                        arguments: null);

            //    string message = "Hello World!";
            //    var body = Encoding.UTF8.GetBytes(message);

            //    channel.BasicPublish(exchange: "",
            //                         routingKey: "hello",
            //                         basicProperties: null,
            //                         body: body);
            //    Console.WriteLine(" [x] Sent {0}", message);
            //}

            //Console.WriteLine(" Press [enter] to exit.");
            //Console.ReadLine();
            #endregion


            #region 工作队列
            ////定义一个连接，指定要连接到哪里
            ////记着配置之前设置的用户名和密码
            //var factory = new ConnectionFactory()
            //{
            //    HostName = "localhost",
            //    UserName = "andy",
            //    Password = "youngdreamer"
            //};
            ////创建连接 
            //using (var connection = factory.CreateConnection())
            ////创建管道
            //using (var channel = connection.CreateModel())
            //{

            //    //定义一个队列
            //    channel.QueueDeclare(queue: "task_queue",
            //                        durable: true,//持久的，设置为true将把消息持久化，保证在rabbitmq停止之后消息不会丢失
            //                        exclusive: false,//独有的，排斥
            //                        autoDelete: false,
            //                        arguments: null);


            //    var message = GetMessage(args);
            //    var body = Encoding.UTF8.GetBytes(message);

            //    var properties = channel.CreateBasicProperties();
            //    properties.Persistent = true;//消息持久化

            //    channel.BasicPublish(exchange: "",
            //                         routingKey: "task_queue",//如果没有用交换，这里保持跟队列名称一致
            //                         basicProperties: properties,
            //                         body: body);

            //    Console.WriteLine(" [x] Sent {0}", message);
            //}

            //Console.WriteLine(" Press [enter] to exit.");
            //Console.ReadLine();

            #endregion

            #region 路由 
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "andy",
                Password = "youngdreamer"
            };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("direct_email", ExchangeType.Direct, true, false, null);

                var severity =args.Length>0?args[0]: "info";
                var message = GetMessage(args);

                var body = Encoding.UTF8.GetBytes(message);

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(exchange: "direct_email",
                                     routingKey: severity,
                                     basicProperties: properties,
                                     body: body);

                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();

            #endregion
        }
        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World!");
        }

    }
}
