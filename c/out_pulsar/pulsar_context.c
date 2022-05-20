
#include <fluent-bit/flb_output_plugin.h>

#include <pulsar/c/authentication.h>
#include <pulsar/c/client.h>
#include "pulsar_context.h"

#define PULSAR_DEFAULT_MEMORY_LIMIT 1024

static int debug_n = 0;

flb_out_pulsar_ctx* flb_out_pulsar_create(struct flb_output_instance *ins, struct flb_config* config)
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

    flb_plg_info(ins, "----->>>>>> at: 01\n");
    flb_out_pulsar_ctx *ctx = flb_calloc(1, sizeof(flb_out_pulsar_ctx));
    if (!ctx) {
        flb_errno();
        flb_plg_error(ins, "calloc pulsar context failed.");
        return NULL;
    }

    flb_plg_info(ins, "----->>>>>> at: 02\n");
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
    ctx->data_schema = FLB_PULSAR_SCHEMA_JSON;
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

    /*
     * config and create pulsar client
     */
    ctx->client_conf = pulsar_client_configuration_create();
    if (ctx->token && 0 < strlen(ctx->token)) {
        ctx->authentication = pulsar_authentication_token_create(ctx->token);
        pulsar_client_configuration_set_auth(ctx->client_conf, ctx->authentication);
    }

    pvalue = flb_output_get_property("MemoryLimit", ins);
    if (pvalue && PULSAR_DEFAULT_MEMORY_LIMIT < (memory_limit = atol(pvalue))) {
        pulsar_client_configuration_set_memory_limit(ctx->client_conf, memory_limit);
    }
    
    ctx->client = pulsar_client_create(ctx->url, ctx->client_conf);
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
    }

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
    }

    // SendTimeout
    pvalue = flb_output_get_property("SendTimeout", ins);
    if (pvalue && 0 < (send_timeout = atol(pvalue))) {
        pulsar_producer_configuration_set_send_timeout(ctx->producer_conf, send_timeout);
    }

    // Batching config
    pvalue = flb_output_get_property("BatchingEnabled", ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, false);
        }
    }
    pvalue = flb_output_get_property("BatchingMaxMessages", ins);
    if (pvalue && 0 < (batch_max_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_messages(ctx->producer_conf, batch_max_msg);
    }
    pvalue = flb_output_get_property("BatchingMaxBytes", ins);
    if (pvalue && 0 < (batch_max_bytes = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(ctx->producer_conf, batch_max_bytes);
    }
    pvalue = flb_output_get_property("BatchingMaxPublishDelay", ins);
    if (pvalue && 0 < (batch_max_delay = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_publish_delay_ms(ctx->producer_conf, batch_max_delay);
    }

    // BlockIfQueueFull
    pvalue = flb_output_get_property("BlockIfQueueFull", ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, false);
        }
    }

    // MaxPendingMessages
    pvalue = flb_output_get_property("MaxPendingMessages", ins);
    if (pvalue && 0 < (max_pending_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages(ctx->producer_conf, max_pending_msg);
    }

    // MaxPendingMessagesAcrossPartitions
    pvalue = flb_output_get_property("MaxPendingMessagesAcrossPartitions", ins);
    if (pvalue && 0 < (max_pending_par = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages_across_partitions(ctx->producer_conf, max_pending_par);
    }

    // create pulsar producer
    err = pulsar_client_create_producer(ctx->client, ctx->topic, ctx->producer_conf, &ctx->producer);
    if (err != pulsar_result_Ok) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "Failed to create pulsar producer: %s\n", pulsar_result_str(err));
        return NULL;
    }
    
    /**
     * parse data schema
     */
    pvalue = flb_output_get_property("DataSchema", ins);
    if (pvalue) {
        if (0 == strcasecmp("json", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_JSON;
        } else if (0 == strcasecmp("msgpack", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_MSGP;
        } else if (0 == strcasecmp("gelf", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_GELF;
        } else {
            flb_plg_warn(ins, "unsupported output schema type: %s", pvalue);
        }
    }

    flb_plg_info(ins, "fluent-bit output plugin for pulsar config:\n"
        "    show progress interval:                 %s\n"
        "    output data schema:                     %s\n"
        "    pulsar url:                             %s\n"
        "    auth token:                             %s\n"
        "    memory limit:                           %s\n"
        "    producer name:                          %s\n"
        "    topic:                                  %s\n"
        "    send timeout:                           %s\n"
        "    compress type:                          %s\n"
        "    initial sequence id:                    %s\n"
        "    max pending messages:                   %s\n"
        "    max pending messages across partitions: %s\n"
        "    partitions routing mode:                %s\n"
        "    hashing schema:                         %s\n"
        "    lazy start partitioned producers:       %s\n"
        "    block if queue full:                    %s\n"
        "    batching enabled:                       %s\n"
        "    batching max messages:                  %s\n"
        "    batching max bytes:                     %s\n"
        "    batching max publish delay:             %s\n"
        "    encryption enabled:                     %s\n"
        "    crypto failure action:                  %s\n",
        ctx->show_interval,
        ctx->data_sechma,
        ctx->url,
        ctx->token,
        pulsar_client_configuration_get_memory_limit(ctx->client_conf),
        pulsar_producer_configuration_get_producer_name(ctx->producer_conf),
        ctx->topic,
        pulsar_producer_configuration_get_send_timeout(ctx->producer_conf),
        pulsar_producer_configuration_get_compression_type(ctx->producer_conf),
        pulsar_producer_configuration_get_initial_sequence_id(ctx->producer_conf),
        pulsar_producer_configuration_get_max_pending_messages(ctx->producer_conf),
        pulsar_producer_configuration_get_max_pending_messages_across_partitions(ctx->producer_conf),
        pulsar_producer_configuration_get_partitions_routing_mode(ctx->producer_conf),
        pulsar_producer_configuration_get_hashing_scheme(ctx->producer_conf),
        pulsar_producer_configuration_get_lazy_start_partitioned_producers(ctx->producer_conf),
        pulsar_producer_configuration_get_block_if_queue_full(ctx->producer_conf),
        pulsar_producer_configuration_get_batching_enabled(ctx->producer_conf),
        pulsar_producer_configuration_get_batching_max_messages(ctx->producer_conf),
        pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes(ctx->producer_conf),
        pulsar_producer_configuration_get_batching_max_publish_delay_ms(ctx->producer_conf),
        pulsar_producer_is_encryption_enabled(ctx->producer_conf),
        pulsar_producer_configuration_get_crypto_failure_action(ctx->producer_conf));

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
