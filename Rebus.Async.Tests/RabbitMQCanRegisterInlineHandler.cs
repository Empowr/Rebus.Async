using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using RabbitMQ.Client;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;
using Headers = Rebus.Messages.Headers;

namespace Rebus.Async.Tests
{
    [TestFixture]
    public class RabbitMQCanRegisterInlineHandler : FixtureBase
    {
        const string InputQueueName = "inline-handlers";
        IBus _bus => _publisher.Bus;
        BuiltinHandlerActivator _publisher;

        public const string ConnectionString = "amqp://localhost";

        private readonly string _publisherQueueName = "UnitTestPublisher";
        private readonly string _subscriber1QueueName = "UnitTestSubscriber";
        protected override void SetUp()
        {
            DeleteQueue(_publisherQueueName);
            DeleteQueue(_subscriber1QueueName);

            _publisher = GetBus(_publisherQueueName
                , null, configurer =>
            {
                configurer.Routing(r => r.TypeBased().Map<SomeRequest>(_subscriber1QueueName));
            });
            _bus.Subscribe<SomeReply>();
        }

        [Test]
        public async Task CanDoRequestReply()
        {
            var activator = GetBus(_subscriber1QueueName);

            activator.Handle<SomeRequest>(async (bus, request) =>
            {
                await bus.Reply(new SomeReply());
            });

            var reply = await _bus.SendRequest<SomeReply>(new SomeRequest());

            Assert.That(reply, Is.Not.Null);
        }

        [Test]
        public async Task CanDoPublishReply()
        {
            var activator = GetBus(_subscriber1QueueName);
            await activator.Bus.Subscribe<SomeRequest>();

            activator.Handle<SomeRequest>(async (bus, request) =>
            {
                await bus.Reply(new SomeReply());
            });

            //await _bus.Publish(new SomeRequest());
            var reply = await _bus.PublishRequest<SomeReply>(new SomeRequest());

            Assert.That(reply, Is.Not.Null);
        }

        [Test]
        public async Task CanDoAdvancedSendRequestReply()
        {
            var activator = GetBus(_subscriber1QueueName);
            await activator.Bus.Subscribe<SomeReply>();

            activator.Handle<SomeRequest>(async (bus, request) =>
            {
                await bus.Reply(new SomeReply());
            });

            //await _bus.Publish(new SomeRequest());
            var reply = await _bus.Advanced.SendRequest<SomeReply>(_subscriber1QueueName, new SomeRequest());

            Assert.That(reply, Is.Not.Null);
        }

        [Test]
        public void SupportsTimeout()
        {
            var correlationIdOfRequest = "IS SET IN THE INLINE HANDLER";

            var activator = GetBus(_subscriber1QueueName);
            activator.Handle<SomeRequest>(async (bus, context, request) =>
            {
                correlationIdOfRequest = context.Headers[Headers.CorrelationId];

                Console.WriteLine("Got request - waiting for 4 seconds...");
                await Task.Delay(4000);
                Console.WriteLine("Done waiting - sending reply (even though it's too late)");

                await bus.Reply(new SomeReply());
            });

            var aggregateException = Assert.Throws<AggregateException>(() =>
            {
                var reply = _bus.SendRequest<SomeReply>(new SomeRequest(), timeout: TimeSpan.FromSeconds(2)).Result;
            });

            Assert.That(aggregateException.InnerException, Is.TypeOf<TimeoutException>());

            var timeoutException = (TimeoutException)aggregateException.InnerException;

            Console.WriteLine(timeoutException);

            Assert.That(timeoutException.Message, Contains.Substring(correlationIdOfRequest));
        }

        BuiltinHandlerActivator GetBus(string queueName, Func<string, Task> handlerMethod = null, Action<RebusConfigurer> configurer = null)
        {
            var activator = new BuiltinHandlerActivator();

            Using(activator);

            if (handlerMethod != null)
            {
                activator.Handle(handlerMethod);
            }

            var config = Configure.With(activator)
                .Transport(t => t.UseRabbitMq(ConnectionString, queueName))
                .Options(o => o.EnableSynchronousRequestReply(replyMaxAgeSeconds: 7));
            configurer?.Invoke(config);
            config.Start();

            return activator;
        }
        public static void DeleteQueue(string queueName)
        {
            var connectionFactory = new ConnectionFactory { Uri = ConnectionString };

            using (var connection = connectionFactory.CreateConnection())
            using (var model = connection.CreateModel())
            {
                model.QueueDelete(queueName);
            }
        }

        public class SomeRequest { }
        public class SomeReply { }
    }
}
