
#include <fluent-bit/flb_output_plugin.h>

#include <pulsar/c/authentication.h>
#include <pulsar/c/client.h>
#include "pulsar_context.h"

#define PULSAR_DEFAULT_MEMORY_LIMIT 1024

char* append_log_text(char *config, const char *key, const char* value)
{
    char *start = config;
    while (*key) {
        *config++ = *key++;
    }
    while (*value) {
        *config++ = *value++;
    }
    *config++ = '\n';

    return config;
}

flb_out_pulsar_ctx* flb_out_pulsar_create(const struct flb_output_instance *ins, const struct flb_config* config)
{
    int ret;
    pulsar_result err;
    const char *pvalue;
    long memory_limit = 0;
    long send_timeout = 0;
    long batch_max_msg = 0;
    long batch_max_bytes = 0;
    long batch_max_delay = 0;
    long max_pending_msg = 0;
    long max_pending_par = 0;
    char config_log[1 << 12] = { 0 };
    char *plog = config_log;

    flb_out_pulsar_ctx *ctx = flb_calloc(1, sizeof(flb_out_pulsar_ctx));
    if (!ctx) {
        flb_errno();
        flb_plg_error(ins, "calloc pulsar context failed.");
        return NULL;
    }

    // init context
    ctx->ins = ins;
    ctx->url = NULL;
    ctx->token = NULL;
    ctx->topic = NULL;
    ctx->client = NULL;
    ctx->producer = NULL;
    ctx->authentication = NULL;
    ctx->client_conf = NULL;
    ctx->producer_conf = NULL;
    ctx->total_number = 0;
    ctx->failed_number = 0;
    ctx->success_number = 0;
    ctx->show_interval = DEFAULT_SHOW_INTERVAL;

    // load config
    ret = flb_output_config_map_set(ins, (void*) ctx);
    if (ret < 0) {
        flb_free(ctx);
        flb_plg_error(ins, "unable to load output configuration.");
        return NULL;
    }

    // check url and topic
    if (ctx->url == NULL || ctx->topic == NULL) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "field 'PulsarUrl' and 'Topic' must be specified.");
        return NULL;
    }

    ctx->url_len = strlen(ctx->url);
    ctx->topic_len = strlen(ctx->topic);

    plog = append_log_text(plog, "init pulsar ok !!!", " config:");
    plog = append_log_text(plog, "    PulsarUrl:                          ", ctx->url);

    /*
     * config and create pulsar client
     */
    ctx->client_conf = pulsar_client_configuration_create();
    if (ctx->token) {
        ctx->token_len = strlen(ctx->token);
        ctx->authentication = pulsar_authentication_token_create(ctx->token);
        pulsar_client_configuration_set_auth(ctx->conf, authentication);
        plog = append_log_text(plog, "    Token:                              ", ctx->token);
    }


    pvalue = flb_output_get_property("MemoryLimit", ins);
    if (pvalue && PULSAR_DEFAULT_MEMORY_LIMIT < (memory_limit = atol(pvalue))) {
        pulsar_client_configuration_set_memory_limit(ctx->conf, memory_limit);
        plog = append_log_text(plog, "    MemoryLimit:                        ", pvalue);
    }
    
    ctx->client = pulsar_client_create(pulsar_url, ctx->conf);
    if (!ctx->client) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "create pulsar client failed !");
        return NULL;
    }
    
    /*
     * config and create pulsar producer
     */
    ctx->producer_conf = pulsar_producer_configuration_create();

    // ProducerName
    pvalue = flb_output_get_property("ProducerName", ins);
    if (pvalue) {
        pulsar_producer_configuration_set_producer_name(ctx->producer_conf, pvalue);
        plog = append_log_text(plog, "    ProducerName:                       ", pvalue);
    }
    
    plog = append_log_text(plog, "    Topic:                              ", pvalue);

    // CompressType
    pvalue = flb_output_get_property("CompressType", ins);
    if (pvalue) {
        if (0 == strcasecmp("NONE", pvalue)) {
            pulsar_producer_configuration_set_compression_type(ctx->producer_conf, pulsar_CompressionNone);
        } else if (0 == strcasecmp("LZ4", pvalue)) {
            pulsar_producer_configuration_set_compression_type(ctx->producer_conf, pulsar_CompressionLZ4);
        } else if (0 == strcasecmp("ZLIB", pvalue)) {
            pulsar_producer_configuration_set_compression_type(ctx->producer_conf, pulsar_CompressionZLib);
        } else if (0 == strcasecmp("ZSTD", pvalue)) {
            pulsar_producer_configuration_set_compression_type(ctx->producer_conf, pulsar_CompressionZSTD);
        } else if (0 == strcasecmp("SNAPPY", pvalue)) {
            pulsar_producer_configuration_set_compression_type(ctx->producer_conf, pulsar_CompressionSNAPPY);
        } else {
            flb_plg_warn(ins, "unsupported pulsar compress type: %s", pvalue);
        }
        plog = append_log_text(plog, "    CompressType:                       ", pvalue);
    }

    // SendTimeout
    pvalue = flb_output_get_property("SendTimeout", ins);
    if (pvalue && 0 < (send_timeout = atol(pvalue))) {
        pulsar_producer_configuration_set_send_timeout(ctx->producer_conf, send_timeout);
        plog = append_log_text(plog, "    SendTimeout:                        ", pvalue);
    }

    // Batching config
    pvalue = flb_output_get_property("BatchingEnabled", ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, false);
        }
        plog = append_log_text(plog, "    BatchingEnabled:                    ", pvalue);
    }
    pvalue = flb_output_get_property("BatchingMaxMessages", ins);
    if (pvalue && 0 < (batch_max_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_messages(ctx->producer_conf, batch_max_msg);
        plog = append_log_text(plog, "    BatchingMaxMessages:                ", pvalue);
    }
    pvalue = flb_output_get_property("BatchingMaxBytes", ins);
    if (pvalue && 0 < (batch_max_bytes = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(ctx->producer_conf, batch_max_bytes);
        plog = append_log_text(plog, "    BatchingMaxBytes:                   ", pvalue);
    }
    pvalue = flb_output_get_property("BatchingMaxPublishDelay", ins);
    if (pvalue && 0 < (batch_max_delay = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_publish_delay_ms(ctx->producer_conf, batch_max_delay);
        plog = append_log_text(plog, "    BatchingMaxPublishDelay:            ", pvalue);
    }

    // BlockIfQueueFull
    pvalue = flb_output_get_property("BlockIfQueueFull", ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, false);
        }
        plog = append_log_text(plog, "    BlockIfQueueFull:                   ", pvalue);
    }

    // MaxPendingMessages
    pvalue = flb_output_get_property("MaxPendingMessages", ins);
    if (pvalue && 0 < (max_pending_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages(ctx->producer_conf, max_pending_msg);
        plog = append_log_text(plog, "    MaxPendingMessages:                 ", pvalue);
    }

    // MaxPendingMessagesAcrossPartitions
    pvalue = flb_output_get_property("MaxPendingMessagesAcrossPartitions", ins);
    if (pvalue && 0 < (max_pending_par = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages_across_partitions(ctx->producer_conf, max_pending_par);
        plog = append_log_text(plog, "    MaxPendingMessagesAcrossPartitions: ", pvalue);
    }

    // create pulsar producer
    err = pulsar_client_create_producer(ctx->client, ctx->topic, ctx->producer_conf, &ctx->producer);
    if (err != pulsar_result_Ok) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "Failed to create pulsar producer: %s\n", pulsar_result_str(err));
        return NULL;
    }
   
    flb_plg_info(ins, config_log);
    return ctx;
}

void flb_out_pulsar_destroy(flb_out_pulsar_ctx* ctx)
{
    if (!ctx) {
        return;
    }

    if (ctx->producer) {
        pulsar_producer_close(ctx->producer);
        pulsar_producer_free(ctx->producer);
    }

    if (ctx->producer_conf) {
        pulsar_producer_configuration_free(ctx->producer_conf);
    }

    if (ctx->client) {
        pulsar_client_close(ctx->client);
        pulsar_client_free(ctx->client);
    }

    if (ctx->authentication) {
        pulsar_authentication_free(ctx->authentication);
    }

    if (ctx->client_conf) {
        pulsar_client_configuration_free(ctx->client_conf);
    }

    if (ctx->url) {
        flb_free(ctx->url);
    }
    if (ctx->token) {
        flb_free(ctx->token);
    }
    if (ctx->topic) {
        flb_free(ctx->topic);
    }

    flb_free(ctx);
}
