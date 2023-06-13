#pragma once

#define DEFAULT_SHOW_INTERVAL  200

#define FLB_PULSAR_SCHEMA_JSON 0
#define FLB_PULSAR_SCHEMA_MSGP 1
#define FLB_PULSAR_SCHEMA_GELF 2

// define pulsar output plugin configuration keys
#define OUTPUT_KEY_SHOW_INTERNAL  "showInterval"
#define OUTPUT_KEY_DATA_SCHEMA  "dataSchema"
#define PULSAR_KEY_MEMORY_LIMIT  "memoryLimitBytes"
#define PULSAR_KEY_BROKER_URL  "pulsarBrokerUrl"
#define PULSAR_KEY_AUTH_TOKEN  "pulsarAuthToken"
#define PULSAR_KEY_ASYNC_SEND  "isAsyncSend"
#define PULSAR_KEY_PRODUCER_NAME  "producerName"
#define PULSAR_KEY_TOPIC_NAME  "topicName"
#define PULSAR_KEY_SEND_TIMEOUT  "sendTimeoutMs"
#define PULSAR_KEY_BATCHING_ENABLED  "batchingEnabled"
#define PULSAR_KEY_BATCHING_MAX_MESSAGES  "batchingMaxMessages"
#define PULSAR_KEY_BATCHING_MAX_BYTES  "batchingMaxBytes"
#define PULSAR_KEY_BATCHING_MAX_DEPLY  "batchingMaxPublishDelayMicros"
#define PULSAR_KEY_BLOCK_IF_QUEUE_FULL  "blockIfQueueFull"
#define PULSAR_KEY_CHUNKING_ENABLED  "chunkingEnabled"
#define PULSAR_KEY_COMPRESSION_TYPE  "compressionType"
#define PULSAR_KEY_CRYPTO_FAILURE_ACTION  "cryptoFailureAction"
#define PULSAR_KEY_HASHING_SCHEMA  "hashingScheme"
#define PULSAR_KEY_MESSAGE_ROUTING_MODE  "messageRoutingMode"
#define PULSAR_KEY_MAX_PENDING_MASSAGES  "maxPendingMessages"
#define PULSAR_KEY_MAX_PENDING_MASSAGES_PARTITIONS  "maxPendingMessagesAcrossPartitions"

// plugin context
typedef struct _flb_out_pulsar_context
{
    char* pulsar_broker_url;
    char* pulsar_auth_token;
    char* pulsar_producer_topic;

    bool is_async;
    uint32_t show_interval;
    uint32_t data_schema;
    uint64_t total_number;
    uint64_t failed_number;
    uint64_t success_number;
    uint64_t discarded_number;

    pulsar_client_t *client;
    pulsar_producer_t *producer;
    pulsar_authentication_t *authentication;
    pulsar_client_configuration_t *client_conf;
    pulsar_producer_configuration_t *producer_conf;

    struct flb_output_instance *ins;
    bool (*send_msg_func)(struct _flb_out_pulsar_context*, const char*, size_t);
} flb_out_pulsar_ctx;

struct pulsar_callback_ctx {
    flb_out_pulsar_ctx *ctx;
    pulsar_message_t *msg;
};

flb_out_pulsar_ctx* flb_out_pulsar_create(struct flb_output_instance *ins, struct flb_config* config);
void flb_out_pulsar_destroy(flb_out_pulsar_ctx* ctx);
