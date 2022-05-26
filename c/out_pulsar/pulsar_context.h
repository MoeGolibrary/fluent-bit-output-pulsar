#pragma once

#define DEFAULT_SHOW_INTERVAL  200

#define FLB_PULSAR_SCHEMA_JSON 0
#define FLB_PULSAR_SCHEMA_MSGP 1
#define FLB_PULSAR_SCHEMA_GELF 2

typedef struct _flb_out_pulsar_context
{
    char* url;
    char* token;
    char* topic;

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
