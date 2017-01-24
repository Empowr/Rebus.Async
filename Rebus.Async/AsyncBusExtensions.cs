﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Bus.Advanced;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.Pipeline.Receive;
using Rebus.Threading;

namespace Rebus.Async
{
    /// <summary>
    /// Configuration and bus extepsions for enabling async/await-based request/reply
    /// </summary>
    public static class AsyncBusExtensions
    {
        static readonly ConcurrentDictionary<string, TimedMessage> Messages = new ConcurrentDictionary<string, TimedMessage>();

        /// <summary>
        /// Enables async/await-based request/reply whereby a request can be sent using the <see cref="SendRequest{TReply}"/> method
        /// which can be awaited for a corresponding reply.
        /// </summary>
        public static void EnableSynchronousRequestReply(this OptionsConfigurer configurer, int replyMaxAgeSeconds = 10)
        {
            configurer.Register(c =>
            {
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var replyMaxAge = TimeSpan.FromSeconds(replyMaxAgeSeconds);
                var step = new ReplyHandlerStep(Messages, rebusLoggerFactory, asyncTaskFactory, replyMaxAge);
                return step;
            });

            configurer.Decorate<IPipeline>(c =>
            {
                var pipeline = c.Get<IPipeline>();
                var step = c.Get<ReplyHandlerStep>();
                return new PipelineStepInjector(pipeline)
                    .OnReceive(step, PipelineRelativePosition.Before, typeof(ActivateHandlersStep));
            });
        }

        /// <summary>
        /// Extension method on <see cref="IBus"/> that allows for asynchronously sending a request and dispatching
        /// the received reply to the continuation.
        /// </summary>
        /// <typeparam name="TReply">Specifies the expected type of the reply. Can be any type compatible with the actually received reply</typeparam>
        /// <param name="bus">The bus instance to use to send the request</param>
        /// <param name="request">The request message</param>
        /// <param name="timeout">Optionally specifies the max time to wait for a reply. If this time is exceeded, a <see cref="TimeoutException"/> is thrown</param>
        /// <returns></returns>
        public static async Task<TReply> SendRequest<TReply>(this IBus bus, object request, TimeSpan? timeout = null)
        {
            var correlationId = $"{ReplyHandlerStep.SpecialCorrelationIdPrefix}:{Guid.NewGuid()}";

            try
            {
                var maxWaitTime = timeout ?? TimeSpan.FromSeconds(5);

                var headers = new Dictionary<string, string>
                {
                    {Headers.CorrelationId, correlationId},
                    {ReplyHandlerStep.SpecialRequestTag, "request"}
                };

                var reply = new TimedMessage();
                Messages[correlationId] = reply;
                
                await bus.Send(request, headers);

                await reply.TaskCompletionSource.Task.WithTimeout(maxWaitTime, $"Did not receive reply for request with correlation ID '{correlationId}' within {maxWaitTime} timeout");

                var message = reply.Message;

                try
                {
                    return (TReply) message.Body;
                }
                catch (InvalidCastException exception)
                {
                    throw new InvalidCastException(
                        $"Could not return message {message.GetMessageLabel()} as a {typeof(TReply)}", exception);
                }
            }
            finally
            {
                TimedMessage reply;
                Messages.TryRemove(correlationId, out reply);
            }
        }

        /// <summary>
        /// Extension method on <see cref="IBus"/> that allows for asynchronously sending a request and dispatching
        /// the received reply to the continuation.
        /// </summary>
        /// <typeparam name="TReply">Specifies the expected type of the reply. Can be any type compatible with the actually received reply</typeparam>
        /// <param name="bus">The bus instance to use to send the request</param>
        /// <param name="request">The request message</param>
        /// <param name="timeout">Optionally specifies the max time to wait for a reply. If this time is exceeded, a <see cref="TimeoutException"/> is thrown</param>
        /// <returns></returns>
        public static async Task<TReply> PublishRequest<TReply>(this IBus bus, object request, TimeSpan? timeout = null)
        {
            var correlationId = $"{ReplyHandlerStep.SpecialCorrelationIdPrefix}:{Guid.NewGuid()}";

            try
            {
                var maxWaitTime = timeout ?? TimeSpan.FromSeconds(5);

                var headers = new Dictionary<string, string>
                {
                    {Headers.CorrelationId, correlationId},
                    {ReplyHandlerStep.SpecialRequestTag, "request"}
                };

                var reply = new TimedMessage();
                Messages[correlationId] = reply;

                await bus.Publish(request, headers);

                await reply.TaskCompletionSource.Task.WithTimeout(maxWaitTime, $"Did not receive reply for request with correlation ID '{correlationId}' within {maxWaitTime} timeout");

                var message = reply.Message;

                try
                {
                    return (TReply)message.Body;
                }
                catch (InvalidCastException exception)
                {
                    throw new InvalidCastException(
                        $"Could not return message {message.GetMessageLabel()} as a {typeof(TReply)}", exception);
                }
            }
            finally
            {
                TimedMessage reply;
                Messages.TryRemove(correlationId, out reply);
            }
        }

        /// <summary>
        /// Extension method on <see cref="IBus" /> that allows for asynchronously sending a request and dispatching
        /// the received reply to the continuation.
        /// </summary>
        /// <typeparam name="TReply">Specifies the expected type of the reply. Can be any type compatible with the actually received reply</typeparam>
        /// <param name="advancedApi">The advanced API.</param>
        /// <param name="destination">The destination.</param>
        /// <param name="request">The request message</param>
        /// <param name="timeout">Optionally specifies the max time to wait for a reply. If this time is exceeded, a <see cref="TimeoutException" /> is thrown</param>
        /// <returns></returns>
        /// <exception cref="InvalidCastException"></exception>
        public static async Task<TReply> SendRequest<TReply>(this IAdvancedApi advancedApi, string destination, object request, TimeSpan? timeout = null)
        {
            var correlationId = $"{ReplyHandlerStep.SpecialCorrelationIdPrefix}:{Guid.NewGuid()}";

            try
            {
                var maxWaitTime = timeout ?? TimeSpan.FromSeconds(5);

                var headers = new Dictionary<string, string>
                {
                    {Headers.CorrelationId, correlationId},
                    {ReplyHandlerStep.SpecialRequestTag, "request"}
                };

                var reply = new TimedMessage();
                Messages[correlationId] = reply;

                await advancedApi.Routing.Send(destination, request, headers);

                await reply.TaskCompletionSource.Task.WithTimeout(maxWaitTime, $"Did not receive reply for request with correlation ID '{correlationId}' within {maxWaitTime} timeout");

                var message = reply.Message;

                try
                {
                    return (TReply)message.Body;
                }
                catch (InvalidCastException exception)
                {
                    throw new InvalidCastException(
                        $"Could not return message {message.GetMessageLabel()} as a {typeof(TReply)}", exception);
                }
            }
            finally
            {
                TimedMessage reply;
                Messages.TryRemove(correlationId, out reply);
            }
        }


    }

    public static class AsyncExtentions
    {
        public static async Task<TResult> WithTimeout<TResult>(this Task<TResult> task, TimeSpan timeout, string errorMessage = null)
        {
            if (task == await Task.WhenAny(task, Task.Delay(timeout)))
            {
                return await task;
            }
            throw new TimeoutException(errorMessage ?? $"Timeout of {timeout}");
        }
    }
}
