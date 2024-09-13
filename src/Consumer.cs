using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using RmqExpressionType = Org.Apache.Rocketmq.ExpressionType;
using RmqFilterExpression = Org.Apache.Rocketmq.FilterExpression;
using RmqMessage = Org.Apache.Rocketmq.Message;
using RmqMessageView = Org.Apache.Rocketmq.MessageView;
using RmqProducer = Org.Apache.Rocketmq.Producer;
using RmqSimpleConsumer = Org.Apache.Rocketmq.SimpleConsumer;

namespace Hestia.RocketMQ5
{
    public sealed class Consumer : RocketMQ5
    {
        private readonly ILogger<Consumer> logger;
        private readonly Lazy<RmqSimpleConsumer> consumer;
        private readonly Lazy<RmqProducer> producer;

        private const string ROCKETMQ_FILTER_CONTENT_DEFAULT = "*";
        private const RmqExpressionType ROCKETMQ_FILTER_TYPE_DEFAULT = RmqExpressionType.Tag;
        private const double ROCKETMQ_AWAIT_DURATION_DEFAULT = 45;
        private const int ROCKETMQ_REVICE_SIZE_DEFAULT = 1;
        private const double ROCKETMQ_REVICE_TIMEOUT_DEFAULT = 30;

        private const long CODE_SUCCESS = 0L;
        private const long CODE_FAILED = 1L;


        public Consumer(IServiceProvider services) : this(ROCKETMQ5, services) { }

        public Consumer(string name, IServiceProvider services) : base(name, services, ROLE_CONSUMER)
        {
            logger = services.GetService<ILogger<Consumer>>();
            var await = TimeSpan.FromSeconds(configuration.GetValue("await", ROCKETMQ_AWAIT_DURATION_DEFAULT));
            var topic = configuration.GetValue<string>("topic", null);
            var group = configuration.GetValue<string>("group", null);
            var content = configuration.GetValue($"filter:content", ROCKETMQ_FILTER_CONTENT_DEFAULT);
            var type = configuration.GetValue($"filter:type", ROCKETMQ_FILTER_TYPE_DEFAULT);

            logger?.LogInformation(string.Join(Environment.NewLine, [$"Topic: {topic}", $"Group: {group}", $"Filter: {type}:{content}", $"Await: {await.TotalMilliseconds}"]));

            producer = new Lazy<RmqProducer>(() => {
                return new RmqProducer.Builder()
                .SetClientConfig(client.Value)
                .Build()
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            });

            consumer = new Lazy<RmqSimpleConsumer>(() => {
                var subscription = new Dictionary<string, RmqFilterExpression>
                { { topic, new RmqFilterExpression(content,type) } };

                return new RmqSimpleConsumer.Builder()
                .SetClientConfig(client.Value)
                .SetConsumerGroup(group)
                .SetAwaitDuration(await)
                .SetSubscriptionExpression(subscription)
                .Build()
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            });

            actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_CONSUMER_GROUP_NAME, group); });
            actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_DESTINATION_SUBSCRIPTION_NAME, $"{type}:{content}"); });
        }

        private static string GetMessageType(RmqMessage message)
        {
            if (string.IsNullOrEmpty(message.MessageGroup) && !message.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_NORMAL; }
            if (!string.IsNullOrEmpty(message.MessageGroup)) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_FIFO; }
            if (message.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_DELAY; }
            return null;
        }

        private static string GetMessageType(RmqMessageView view)
        {
            if (string.IsNullOrEmpty(view.MessageGroup) && !view.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_NORMAL; }
            if (!string.IsNullOrEmpty(view.MessageGroup)) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_FIFO; }
            if (view.DeliveryTimestamp.HasValue) { return TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_DELAY; }
            return null;
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task> next)
        {
            await ReceiveAsync(async (view, parent) => {
                await next.Invoke(view, parent);
                return (CODE_SUCCESS, (DateTime?)null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<TimeSpan?>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var delay = await next.Invoke(view, parent);
                return (delay.HasValue ? CODE_FAILED : CODE_SUCCESS, delay.HasValue ? DateTime.Now.Add(delay.Value) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<DateTime?>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var delay_at = await next.Invoke(view, parent);
                return (delay_at.HasValue ? CODE_FAILED : CODE_SUCCESS, delay_at);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(long?, long?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (delay_unix_ts, @base) = await next.Invoke(view, parent);
                return (delay_unix_ts.HasValue ? CODE_FAILED : CODE_SUCCESS, delay_unix_ts.HasValue ? GetDateTimeFromUnixTimestamp(delay_unix_ts.Value, @base ?? TimeSpan.TicksPerSecond) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<bool>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var ok = await next.Invoke(view, parent);
                return (ok ? CODE_SUCCESS : CODE_FAILED, (DateTime?)null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(bool, TimeSpan?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (ok, delay) = await next.Invoke(view, parent);
                return (ok ? CODE_SUCCESS : CODE_FAILED, delay.HasValue ? DateTime.Now.Add(delay.Value) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(bool, DateTime?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (ok, delay_at) = await next.Invoke(view, parent);
                return (ok ? CODE_SUCCESS : CODE_FAILED, delay_at);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(bool, long?, long?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (ok, delay_unix_ts, @base) = await next.Invoke(view, parent);
                return (ok ? CODE_SUCCESS : CODE_FAILED, delay_unix_ts.HasValue ? GetDateTimeFromUnixTimestamp(delay_unix_ts.Value, @base ?? TimeSpan.TicksPerSecond) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<long>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var code = await next.Invoke(view, parent);
                return (code, (DateTime?)null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(long, TimeSpan?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (code, delay) = await next.Invoke(view, parent);
                return (code, delay.HasValue ? DateTime.Now.Add(delay.Value) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(long, long?, long?)>> next)
        {
            await ReceiveAsync(async (view, parent) => {
                var (code, delay_unix_ts, @base) = await next.Invoke(view, parent);
                return (code, delay_unix_ts.HasValue ? GetDateTimeFromUnixTimestamp(delay_unix_ts.Value, @base ?? TimeSpan.TicksPerSecond) : null);
            });
        }

        public async Task ReceiveAsync(Func<RmqMessageView, string, Task<(long, DateTime?)>> next)
        {
            var size = configuration.GetValue("size", ROCKETMQ_REVICE_SIZE_DEFAULT);
            var duration = TimeSpan.FromSeconds(configuration.GetValue("duration", ROCKETMQ_REVICE_TIMEOUT_DEFAULT));

            var (receive, views) = await ReceiveAsync(size, duration);

            foreach (var view in views)
            {
                var ack = await AckAsync(view, receive);

                var (code, delay_at) = await ProcessAsync(view, next, ack);

                if (code == CODE_SUCCESS) { continue; }

                await ReSendAsync(view, delay_at, ack);
            }
        }

        private async Task<(Activity, List<RmqMessageView>)> ReceiveAsync(int size, TimeSpan duration)
        {
            using var activity = StartActivity(TRACE_FUNCTION_RECEIVE, ActivityKind.Consumer, null, TRACE_MESSAGING_OPERATION_TYPE_RECEIVE, TRACE_MESSAGING_OPERATION_NAME_RECEIVE);
            activity?.SetTag(TRACE_MESSAGING_BATCH_RECEIVE_SIZE, size);
            activity?.SetTag(TRACE_MESSAGING_BATCH_RECEIVE_DURATION, duration.Ticks);

            var views = await consumer.Value.Receive(size, duration);
            logger?.LogDebug(string.Join(" <<<< ", TRACE_FUNCTION_RECEIVE, $"Size: {size} | Duration: {duration.TotalMilliseconds}", $"Count: {views.Count}"));

            activity?.SetTag(TRACE_MESSAGING_BATCH_MESSAGE_COUNT, views.Count);

            return (activity, views);
        }

        private async Task<Activity> AckAsync(RmqMessageView view, Activity receive)
        {
            using var activity = StartActivity(TRACE_FUNCTION_ACK, ActivityKind.Consumer, receive?.Id, TRACE_MESSAGING_OPERATION_TYPE_RECEIVE, TRACE_MESSAGING_OPERATION_NAME_ACK);
            await consumer.Value.Ack(view);
            logger?.LogDebug(string.Join(" >>>> ", TRACE_FUNCTION_ACK, view));

            activity?.SetTag(TRACE_MESSAGING_MESSAGE_ID, view.MessageId);

            return activity;
        }

        private async Task<(long, DateTime?)> ProcessAsync(RmqMessageView view, Func<RmqMessageView, string, Task<(long, DateTime?)>> next, Activity ack)
        {
            var tid = configuration.GetValue("tid", PROPERTY_TRACE_ID_DEFAULT);
            var aid = view.Properties.GetValueOrDefault(tid, null);
            using var activity = StartActivity(TRACE_FUNCTION_PROCESS, ActivityKind.Consumer, aid ?? ack?.Id, TRACE_MESSAGING_OPERATION_TYPE_PROCESS, TRACE_MESSAGING_OPERATION_NAME_PROCESS, ack);

            if (activity is not null)
            {
                logger?.LogTrace(string.Join(" # ", ["Export", tid, aid, ack?.Id, activity.ParentId]));
                activity.SetTag(TRACE_MESSAGING_MESSAGE_ID, view.MessageId);
                var type = GetMessageType(view);
                if (!string.IsNullOrEmpty(type)) { activity.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE, type); }
                activity.SetTag(TRACE_MESSAGING_DESTINATION_PARTITION_ID, view.Topic);
                activity.SetTag(TRACE_MESSAGING_MESSAGE_BODY_SIZE, view.Body.Length);
                if (!string.IsNullOrEmpty(view.Tag)) { activity.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_TAG, view.Tag); }
                if (view.Keys?.Count > 0) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_KEYS, Format(view.Keys)); }
                if (view.DeliveryTimestamp.HasValue) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP, GetUnixTimestampFromDateTime(view.DeliveryTimestamp)); }
                if (!string.IsNullOrEmpty(view.MessageGroup)) { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_MESSAGE_GROUP, view.MessageGroup); }
            }

            var (code, delay_at) = await next.Invoke(view, activity?.Id);

            logger?.LogInformation(string.Join(" <<<< ", TRACE_FUNCTION_PROCESS, delay_at.HasValue ? $"Code: {code} | Delay: {delay_at.Value:yyyyMMddHHmmss}" : $"Code: {code}", view));

            activity?.SetTag(TRACE_MESSAGING_MESSAGE_PROCESS_CODE, code);

            if (delay_at.HasValue) { activity?.SetTag(TRACE_MESSAGING_MESSAGE_PROCESS_DELIVERY_TIMESTAMP, GetUnixTimestampFromDateTime(delay_at)); };

            return (code, delay_at);
        }

        private async Task ReSendAsync(RmqMessageView view, DateTime? delay_at, Activity ack)
        {
            using var activity = StartActivity(TRACE_FUNCTION_RESEND, ActivityKind.Consumer, ack?.Id, TRACE_MESSAGING_OPERATION_TYPE_RECEIVE, TRACE_MESSAGING_OPERATION_NAME_SEND);

            var builder = new RmqMessage.Builder().SetTopic(view.Topic).SetBody(view.Body);
            if (!string.IsNullOrEmpty(view.Tag))
            {
                builder.SetTag(view.Tag);
            }
            if (!string.IsNullOrEmpty(view.MessageGroup))
            {
                builder.SetMessageGroup(view.MessageGroup);
            }
            if (delay_at.HasValue)
            {
                builder.SetDeliveryTimestamp(delay_at.Value);
            }
            if (view.Keys.Count > 0)
            {
                builder.SetKeys(view.Keys.ToArray());
            }
            foreach (var property in view.Properties)
            {
                builder.AddProperty(property.Key, property.Value);
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

            var receipt = await producer.Value.Send(message);

            logger?.LogInformation(string.Join(" >>>> ", TRACE_FUNCTION_RESEND, $"Receipt: {receipt.MessageId}", view, message));

            activity?.SetTag(TRACE_MESSAGING_MESSAGE_ID, receipt.MessageId);
        }
    }
}
