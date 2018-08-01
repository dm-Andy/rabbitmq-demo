using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading;

namespace RabbitMQ.Demo.Receive
{
    class Receive
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
            //using (var connection = factory.CreateConnection())
            //using (var channel = connection.CreateModel())
            //{
            //    //在这里先定义了一个队列，保证了在接受者获取消息时队列是存在的
            //    channel.QueueDeclare(queue: "hello",
            //                         durable: false,
            //                         exclusive: false,
            //                         autoDelete: false,
            //                         arguments: null);

            //    var consumer = new EventingBasicConsumer(channel);
            //    consumer.Received += (model, ea) =>
            //    {
            //        var body = ea.Body;
            //        var message = Encoding.UTF8.GetString(body);
            //        Console.WriteLine(" [x] Received {0}", message);
            //    };
            //    //Consume-消耗
            //    channel.BasicConsume(queue: "hello",
            //                         noAck: true,//确认
            //                         consumer: consumer);

            //    Console.WriteLine(" Press [enter] to exit.");
            //    Console.ReadLine();
            //}


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
            //using (var connection = factory.CreateConnection())
            //using (var channel = connection.CreateModel())
            //{
            //    //在这里先定义了一个队列，保证了在接受者获取消息时队列是存在的
            //    channel.QueueDeclare(queue: "task_queue",
            //                         durable: true,
            //                         exclusive: false,
            //                         autoDelete: false,
            //                         arguments: null); 

            //    //设置在一个时间内只给一个worker分发一个任务
            //    channel.BasicQos(0, 1, false);

            //    var consumer = new EventingBasicConsumer(channel);
            //    //会一直监听这个，在走完下面代码之后如果有消息就对执行这个方法
            //    consumer.Received += (model, ea) =>
            //    {
            //        var body = ea.Body;
            //        var message = Encoding.UTF8.GetString(body);
            //        Console.WriteLine(" [x] Received {0}", message);

            //        int dots = message.Split('.').Length - 1;
            //        Thread.Sleep(dots * 1000);

            //        Console.WriteLine(" [x] Done");

            //        //不要忘记这句话，否则rabbitmq会很吃内存
            //        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            //    };
            //    //noAck 是否开启消费者响应，默认是false开启的 
            //    //一旦完成一个任务，发送一个响应，标识任务完成
            //    //保证了任务不会丢失
            //    //channel.BasicConsume(queue: "task_queue", noAck: true, consumer: consumer);
            //    channel.BasicConsume(queue: "task_queue", noAck: false, consumer: consumer);//这个在第一次执行一遍

            //    Console.WriteLine(" Press [enter] to exit.");
            //    Console.ReadLine();
            //}
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
                var queueName =  channel.QueueDeclare("queue_email",true,false,false,null).QueueName;
                //1.一个队列绑定多个路由，一个消费者绑定一个消息队列处理多个路由
                //2.如果相同的代码开启多个消费者控制台程序，那么将会有两个管道，但是消息队列和交换器都是一样的，
                //也就是说有多个消费者同时监听一个交换器绑定的一个消息队列
                new List<string> { "info", "debug", "warn" }.ForEach(x =>
                {
                    channel.QueueBind(queue: queueName, exchange: "direct_email", routingKey: x);
                });

                //这个是设置一个消费者只能接收一个任务，只有这个任务完成后才会被分发新的任务，
                //如果消费者都在忙，那么消息队列中的消息不会取出，只有有空闲的消费者的时候才会取出进行分发
                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    var routingKey = ea.RoutingKey;

                    Console.WriteLine(" [x] Received '{0}':'{1}'",
                                      routingKey, message);
                    Thread.Sleep(5000);
                    Console.WriteLine(" [x] Done");
                    //不要忘记这句话，否则rabbitmq会很吃内存
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                //noAck 是否开启消费者响应，默认是false开启的 
                //一旦完成一个任务，发送一个响应，标识任务完成
                //保证了任务不会丢失
                channel.BasicConsume(queue: queueName, noAck: false, consumer: consumer);//这个在第一次执行一遍

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
            #endregion
        }
    }
}
