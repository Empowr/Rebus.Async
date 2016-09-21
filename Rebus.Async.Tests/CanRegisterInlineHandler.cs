using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Contracts;
using Rebus.Transport.InMem;

namespace Rebus.Async.Tests
{
    [TestFixture]
    public class CanRegisterInlineHandler : FixtureBase
    {
        const string InputQueueName = "inline-handlers";
        IBus _bus;
        BuiltinHandlerActivator _activator;

        protected override void SetUp()
        {
            _activator = new BuiltinHandlerActivator();

            Using(_activator);

            _bus = Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), InputQueueName))
                .Options(o => o.EnableSynchronousRequestReply(replyMaxAgeSeconds: 7))
                .Routing(r =>
                {
                    var config = r.TypeBased();
                    config.Map<SomeRequest>(InputQueueName);
                    config.Map<SomeReply>(InputQueueName);
                }
                )
                .Start();
        }

        [Test]
        public async Task CanDoRequestReply()
        {
            _activator.Handle<SomeRequest>(async (bus, request) =>
            {
                await bus.Reply(new SomeReply());
            });

            var reply = await _bus.SendRequest<SomeReply>(new SomeRequest());

            Assert.That(reply, Is.Not.Null);
        }

        [Test]
        public async Task CanDoPublishReply()
        {
            await _activator.Bus.Subscribe<SomeReply>();
            await _activator.Bus.Subscribe<SomeRequest>();

            _activator.Handle<SomeReply>(async (bus, request) =>
            {
                await Task.Delay(1000);
            });

            _activator.Handle<SomeRequest>(async (bus, request) =>
            {
                await bus.Reply(new SomeReply());
            });

            var reply = await _bus.PublishRequest<SomeReply>(new SomeRequest());

            Assert.That(reply, Is.Not.Null);
        }

        [Test]
        public void SupportsTimeout()
        {
            var correlationIdOfRequest = "IS SET IN THE INLINE HANDLER";

            _activator.Handle<SomeRequest>(async (bus, context, request) =>
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

        public class SomeRequest { }
        public class SomeReply { }
    }
}
