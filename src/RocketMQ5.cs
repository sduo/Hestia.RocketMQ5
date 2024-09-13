using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using RmqClientConfig = Org.Apache.Rocketmq.ClientConfig;
using RmqStaticSessionCredentialsProvider = Org.Apache.Rocketmq.StaticSessionCredentialsProvider;


namespace Hestia.RocketMQ5
{
    public class RocketMQ5
    {
        protected const string ROCKETMQ5 = "RocketMQ5";
        public readonly string[] TRACE_SOURCE = [ROCKETMQ5, $"{ROCKETMQ5}.*"];
        protected const string PROPERTY_TRACE_ID_DEFAULT = "TRACE_ID";
        protected const string ROLE_PRODUCER = "Producer";
        protected const string ROLE_CONSUMER = "Consumer";
        protected const string TRACE_FUNCTION_SEND = "Send";
        protected const string TRACE_FUNCTION_RECEIVE = "Receive";
        protected const string TRACE_FUNCTION_ACK = "Ack";
        protected const string TRACE_FUNCTION_PROCESS = "Process";
        protected const string TRACE_FUNCTION_RESEND = "ReSend";


        protected const string TRACE_MESSAGING_SYSTEM = "messaging.system";
        protected const string TRACE_MESSAGING_SYSTEM_ROCKETMQ = "rocketmq";
        protected const string TRACE_MESSAGING_CLIENT_ID = "messaging.client.id";


        protected const string TRACE_SERVER_ADDRESS = "server.address";

        protected const string TRACE_MESSAGING_DESTINATION_NAME = "messaging.destination.name";
        protected const string TRACE_MESSAGING_DESTINATION_PARTITION_ID = "messaging.destination.partition.id";
        protected const string TRACE_MESSAGING_DESTINATION_SUBSCRIPTION_NAME = "messaging.destination.subscription.name";


        protected const string TRACE_MESSAGING_OPERATION_TYPE = "messaging.operation.type";
        protected const string TRACE_MESSAGING_OPERATION_TYPE_PUBLISH = "publish";
        protected const string TRACE_MESSAGING_OPERATION_TYPE_PROCESS = "process";
        protected const string TRACE_MESSAGING_OPERATION_TYPE_RECEIVE = "receive";

        protected const string TRACE_MESSAGING_OPERATION_NAME = "messaging.operation.name";
        protected const string TRACE_MESSAGING_OPERATION_NAME_SEND = "send";
        protected const string TRACE_MESSAGING_OPERATION_NAME_PROCESS = "process";
        protected const string TRACE_MESSAGING_OPERATION_NAME_RECEIVE = "receive";
        protected const string TRACE_MESSAGING_OPERATION_NAME_ACK = "ack";

        protected const string TRACE_MESSAGING_BATCH_MESSAGE_COUNT = "messaging.batch.message_count";
        protected const string TRACE_MESSAGING_BATCH_RECEIVE_SIZE = "messaging.batch.receive_size";
        protected const string TRACE_MESSAGING_BATCH_RECEIVE_DURATION = "messaging.batch.receive_duration";

        protected const string TRACE_MESSAGING_CONSUMER_GROUP_NAME = "messaging.consumer.group.name";


        protected const string TRACE_MESSAGING_MESSAGE_BODY_SIZE = "messaging.message.body.size";
        protected const string TRACE_MESSAGING_MESSAGE_ID = "messaging.message.id";
        protected const string TRACE_MESSAGING_MESSAGE_CONVERSATION_ID = "messaging.message.conversation_id";
        protected const string TRACE_MESSAGING_MESSAGE_PROCESS_CODE = "messaging.message.process.code";
        protected const string TRACE_MESSAGING_MESSAGE_PROCESS_DELIVERY_TIMESTAMP = "messaging.message.process.delivery_timestamp";

        protected const string TRACE_MESSAGING_ROCKETMQ_NAMESPACE = "messaging.rocketmq.namespace";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE = "messaging.rocketmq.message.type";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_DELAY = "delay";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_FIFO = "fifo";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_NORMAL = "normal";
        //protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TYPE_TRANSACTION = "transaction";

        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TAG = "messaging.rocketmq.message.tag";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_KEYS = "messaging.rocketmq.message.keys";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP = "messaging.rocketmq.message.delivery_timestamp";
        protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_GROUP = "messaging.rocketmq.message.group";
        //protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION = "messaging.rocketmq.message.transaction";
        //protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION_COMMIT = "commit";
        //protected const string TRACE_MESSAGING_ROCKETMQ_MESSAGE_TRANSACTION_ROLLBACK = "rollback";

        private readonly ILogger<RocketMQ5> logger;
        protected readonly IConfiguration configuration;

        private const long UnixZeroTicks = 621355968000000000;

        protected readonly Lazy<RmqClientConfig> client;

        private readonly Lazy<ActivitySource> tracer;

        protected readonly List<Action<Activity>> actions = [];

        protected static long GetUnixTimestampFromDateTime(DateTime? dt, long @base = TimeSpan.TicksPerMillisecond)
        {
            return dt.HasValue ? GetUnixTimestampFromTicks(dt.Value.Ticks, @base) : 0L;
        }

        protected static DateTime GetDateTimeFromUnixTimestamp(long timestamp, long @base = TimeSpan.TicksPerMillisecond)
        {            
            return new DateTime(UnixZeroTicks + timestamp * @base);
        }

        protected static long GetUnixTimestampFromTicks(long ticks, long @base = TimeSpan.TicksPerMillisecond)
        {
            return (ticks - UnixZeroTicks) / @base;
        }

        protected static string Format(IEnumerable<string> strings)
        {
            return Format(strings, StringFormatter);
        }

        private static string StringFormatter(string source)
        {
            return $"\"{source}\"";
        }

        private static string Format<T>(IEnumerable<T> collection, Func<T, string> formatter)
        {
            return collection is null ? string.Empty : string.Concat("[", string.Join(",", collection.Select(formatter)), "]");
        }

        protected RocketMQ5(string name, IServiceProvider services, string role)
        {
            logger = services.GetService<ILogger<RocketMQ5>>();
            configuration = services.GetRequiredService<IConfiguration>().GetSection(name);

            var endpoints = configuration.GetValue<string>("endpoints", null);
            var instance = configuration.GetValue("instance", endpoints?.Split('.', 2)[0] ?? string.Empty);
            var client = configuration.GetValue("client", $"{Environment.MachineName}#{Environment.ProcessId}");
            var conversation = configuration.GetValue("conversation", Environment.ProcessPath);
            var @namespace = configuration.GetValue("namespace", ROCKETMQ5);
            var ak = configuration.GetValue<string>("ak", null);
            var sk = configuration.GetValue<string>("sk", null);


            logger?.LogInformation(string.Join(Environment.NewLine, [$"Role: {role}", $"Endpoint: {endpoints}", $"Instance: {instance}", $"Client: {client}", $"Conversation: {conversation}", $"Namespace: {@namespace}", $"AK: {ak}"]));

            tracer = new Lazy<ActivitySource>(() => {
                return new ActivitySource($"{ROCKETMQ5}.{role}");
            });

            actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_SYSTEM, TRACE_MESSAGING_SYSTEM_ROCKETMQ); });
            actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_CLIENT_ID, client); });
            actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_MESSAGE_CONVERSATION_ID, conversation); });
            actions.Add((activity) => { activity?.SetTag(TRACE_SERVER_ADDRESS, endpoints); });
            if (!string.IsNullOrEmpty(@namespace)) { actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_ROCKETMQ_NAMESPACE, @namespace); }); }
            if (!string.IsNullOrEmpty(instance)) { actions.Add((activity) => { activity?.SetTag(TRACE_MESSAGING_DESTINATION_NAME, instance); }); }

            this.client = new Lazy<RmqClientConfig>(() => {
                var credentials = new RmqStaticSessionCredentialsProvider(ak, sk);
                return new RmqClientConfig.Builder()
                    .SetEndpoints(endpoints)
                    .SetCredentialsProvider(credentials)
                    .Build();
            });
        }

        protected Activity StartActivity(string function_name, ActivityKind kind, string parent, string operation_type, string operation_name, params Activity[] links)
        {
            var activity = tracer.Value.StartActivity(
                name: $"{tracer.Value.Name}.{function_name}",
                parentId: parent,
                kind: kind,
                links: links.Where((link) => { return link is not null; }).Select((link) => { return new ActivityLink(link.Context); })
            );

            if (activity is null) { return null; }

            activity.SetTag(TRACE_MESSAGING_OPERATION_TYPE, operation_type);
            activity.SetTag(TRACE_MESSAGING_OPERATION_NAME, operation_name);
            foreach (var action in actions) { action.Invoke(activity); }

            return activity;
        }
    }
}
