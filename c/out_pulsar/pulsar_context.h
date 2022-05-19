#pragma once

#define DEFAULT_SHOW_INTERVAL 200

typedef struct _flb_out_pulsar_context
{
    char* url;
    char* token;
    char* topic;
    size_t url_len;
    size_t token_len;
    size_t topic_len;

    uint64_t total_number;
    uint64_t failed_number;
    uint64_t success_number;
    unsigned int show_interval;

    pulsar_client_t *client;
    pulsar_producer_t *producer;
    pulsar_authentication_t *authentication;
    pulsar_client_configuration_t *client_conf;
    pulsar_producer_configuration_t *producer_conf;

    struct flb_output_instance *ins;
} flb_out_pulsar_ctx;

flb_out_pulsar_ctx* flb_out_pulsar_create(struct flb_output_instance *ins, struct flb_config* config);
void flb_out_pulsar_destroy(flb_out_pulsar_ctx* ctx);
