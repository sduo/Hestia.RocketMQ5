using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using RmqISendReceipt = Org.Apache.Rocketmq.ISendReceipt;
using RmqMessage = Org.Apache.Rocketmq.Message;
using RmqProducer = Org.Apache.Rocketmq.Producer;

namespace Hestia.RocketMQ5
{
    public sealed class Producer : RocketMQ5
    {
        private readonly ILogger<Producer> logger;
        private readonly Lazy<RmqProducer> producer;

        public Producer(IServiceProvider services) : this(ROCKETMQ5, services) { }
        public Producer(string name, IServiceProvider services) : base(name, services, ROLE_PRODUCER)
        {
            logger = services.GetService<ILogger<Producer>>();

            producer = new Lazy<RmqProducer>(() => {
                return new RmqProducer.Builder()
                .SetClientConfig(client.Value)
                .Build()
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            });
        }

        private static string GetMessageType(RmqMessage message)
        {
            if (string.IsNullOrEmpty(message.MessageGroup) && !message.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_NORMAL; }
            if (!string.IsNullOrEmpty(message.MessageGroup)) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_FIFO; }
            if (message.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_DELAY; }
            return null;
        }

        public async Task<string> SendAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, string parent = null, params Activity[] links)
        {
            var receipt = await SendAsync(topic, body, tag, keys, properties, null, null, parent);
            return receipt.MessageId;
        }

        public async Task<string> SendFifoAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, string group = null, string parent = null, params Activity[] links)
        {
            var receipt = await SendAsync(topic, body, tag, keys, properties, null, group, parent);
            return receipt.MessageId;
        }

        public async Task<string> SendDelayAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, TimeSpan? delay = null, string parent = null, params Activity[] links)
        {
            var receipt = await SendAsync(topic, body, tag, keys, properties, delay.HasValue ? DateTime.Now.Add(delay.Value) : null, null, parent);
            return receipt.MessageId;
        }

        public async Task<string> SendDelayAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, DateTime? delay_at = null, string parent = null, params Activity[] links)
        {
            var receipt = await SendAsync(topic, body, tag, keys, properties, delay_at, null, parent);
            return receipt.MessageId;
        }

        public async Task<string> SendDelayAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, long? delay_unix_ts = null, long @base = TimeSpan.TicksPerSecond, string parent = null, params Activity[] links)
        {
            var receipt = await SendAsync(topic, body, tag, keys, properties, delay_unix_ts.HasValue ? GetDateTimeFromUnixTimestamp(delay_unix_ts.Value, @base) : null, null, parent);
            return receipt.MessageId;
        }

        private async Task<RmqISendReceipt> SendAsync(string topic, byte[] body, string tag = null, string[] keys = null, IDictionary<string, string> properties = null, DateTime? delay_at = null, string group = null/*,Func<bool> confirm = null*/, string parent = null, params Activity[] links)
        {
            using var activity = StartActivity(TRACE_FUNCTION_SEND, ActivityKind.Producer, null, TRACE_MESSAGING_OPERATION_TYPE_PUBLISH, TRACE_MESSAGING_OPERATION_NAME_SEND);

            var builder = new RmqMessage.Builder().SetTopic(topic).SetBody(body);
            if (!string.IsNullOrEmpty(tag)) { builder.SetTag(tag); }
            if (keys?.Length > 0) { builder.SetKeys(keys); }
            if (properties?.Count > 0)
            {
                foreach (var property in properties)
                {
                    builder.AddProperty(property.Key, property.Value);
                }
            }
            if (!string.IsNullOrEmpty(group)) { builder.SetMessageGroup(group); }
            if (delay_at.HasValue) { builder.SetDeliveryTimestamp(delay_at.Value); }

            if (activity is not null)
            {
                var tid = configuration.GetValue("tid", PROPERTY_TRACE_ID_DEFAULT);
                builder.AddProperty(tid, activity.Id);
                logger?.LogTrace(string.Join(" # ", ["Inject", tid, activity.Id]));
            }

            var message = builder.Build();

            if (activity is not null)
            {
                activity.SetTag(TRACE_MESSAGING_BATCH_MESSAGE_COUNT, 1);
                var type = GetMessageType(message);
                if (!string.IsNullOrEmpty(type)) { activity.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE, type); }
                activity.SetTag(TRACE_MESSAGING_DESTINATION_PARTITION_ID, message.Topic);
                activity.SetTag(TRACE_MESSAGING_MESSAGE_BODY_SIZE, message.Body.Length);
                if (!string.IsNullOrEmpty(message.Tag)) { activity.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_TAG, message.Tag); }
                if (message.Keys?.Count > 0) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_KEYS, Format(message.Keys)); }
                if (message.DeliveryTimestamp.HasValue) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, GetUnixTimestampFromDateTime(message.DeliveryTimestamp)); }
                if (!string.IsNullOrEmpty(message.MessageGroup)) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_GROUP, message.MessageGroup); }
            }

            //var transaction = confirm is null ? null : producer.Value.BeginTransaction();

            var receipt = await producer.Value.Send(message);

            logger?.LogInformation(string.Join(" >>>> ", TRACE_FUNCTION_SEND, $"Receipt: {receipt.MessageId}", message));

            activity?.SetTag(TRACE_MESSAGING_MESSAGE_ID, receipt.MessageId);

            //if(confirm is not null && transaction is not null)
            //{
            //    var commit = confirm.Invoke();
            //    activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION, commit ? TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION_COMMIT: TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION_ROLLBACK);
            //    if (commit) { transaction.Commit(); } else { transaction.Rollback(); }
            //}

            return receipt;
        }
    }
}
