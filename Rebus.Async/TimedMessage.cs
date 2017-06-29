using System;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.Async
{
    class TimedMessage
    {
        public TaskCompletionSource<object> TaskCompletionSource { get; set; }

        public TimedMessage(Message message): this()
        {
            Message = message;
            Time = DateTime.UtcNow;
        }

        public TimedMessage()
        {
            TaskCompletionSource = new TaskCompletionSource<object>();
        }

        public Message Message { get; set; }
        public DateTime Time { get; }
        public TimeSpan Age => DateTime.UtcNow - Time;
        //public ITransactionContext AmbientContext { get; set; }
    }
}