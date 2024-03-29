#include <fluent-bit/flb_output_plugin.h>

#include <pulsar/c/version.h>
#include <pulsar/c/authentication.h>
#include <pulsar/c/client.h>
#include "pulsar_context.h"

#define PULSAR_DEFAULT_MEMORY_LIMIT 8088608
#define PULSAR_AUTH_TOKEN_MASK_LEN  16

void flb_pulsar_send_callback(pulsar_result code, pulsar_message_id_t *msgId, void *data)
{
    struct pulsar_callback_ctx *pcctx = (struct pulsar_callback_ctx*)data;
    if (pulsar_result_Ok == code) {
        // ...
    } else {
        flb_plg_info(pcctx->ctx->ins, "pulsar discard message: %s, msg: %s", pulsar_result_str(code), pulsar_message_get_data(pcctx->msg));
        ++pcctx->ctx->discarded_number;
    }
    if (NULL != msgId) {
        pulsar_message_id_free(msgId);
    }

    pulsar_message_free(pcctx->msg);
    flb_free(pcctx);
}

// send messages synchronously
bool pulsar_send_msg(flb_out_pulsar_ctx *ctx, const char* data, size_t len) {
    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, data, len);
    pulsar_result ret = pulsar_producer_send(ctx->producer, message);
    pulsar_message_free(message);

    if (pulsar_result_Ok == ret) {
        return true;
    } else {
        flb_plg_info(ctx->ins, "pulsar publish message failed: %s, msg: %s", pulsar_result_str(ret), data);
        return false;
    }
}

// send messages asynchronously
bool pulsar_async_send(flb_out_pulsar_ctx *ctx, const char* data, size_t len) {
    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, data, len);

    struct pulsar_callback_ctx *pcctx = flb_calloc(1, sizeof(struct pulsar_callback_ctx));
    pcctx->ctx = ctx;
    pcctx->msg = message;
    
    pulsar_producer_send_async(ctx->producer, message, flb_pulsar_send_callback, pcctx);
    return true;
}

const char* get_msg_send_async(flb_out_pulsar_ctx *ctx) {
    return ctx->is_async ? "true" : "false";
}
const uint32_t get_config_show_interval(flb_out_pulsar_ctx *ctx) {
    return ctx->show_interval;
}
const char* get_config_output_schema(flb_out_pulsar_ctx *ctx) {
    switch (ctx->data_schema)
    {
    case FLB_PULSAR_SCHEMA_MSGP:
        return "MSGPACK";
    case FLB_PULSAR_SCHEMA_GELF:
        return "GELF";
    default:
        return "JSON";
    }
}
const char* get_pulsar_url(flb_out_pulsar_ctx *ctx) {
    return ctx->pulsar_broker_url;
}
void mask_memory(char* buf, size_t size, const char *src, size_t len, size_t n) {
    memcpy(buf, src, n);
    memset(buf + n, '*', size - (n << 1));
    memcpy(buf + size - n, src + (len - n), n);
}
static const char* get_pulsar_auth_token(flb_out_pulsar_ctx *ctx) {
    static char text[PULSAR_AUTH_TOKEN_MASK_LEN + 1] = { 0 };
    if (ctx->pulsar_auth_token) {
        size_t len = strlen(ctx->pulsar_auth_token);
        if (len  < 4) {
            memset(text, '*', PULSAR_AUTH_TOKEN_MASK_LEN);
        } else if (len < 8) {
            mask_memory(text, PULSAR_AUTH_TOKEN_MASK_LEN, ctx->pulsar_auth_token, len, 2);
        } else if (len < 16) {
            mask_memory(text, PULSAR_AUTH_TOKEN_MASK_LEN, ctx->pulsar_auth_token, len, 4);
        } else {
            mask_memory(text, PULSAR_AUTH_TOKEN_MASK_LEN, ctx->pulsar_auth_token, len, 6);
        }
    }
    
    return text;
}
uint64_t get_pulsar_memory_limit(flb_out_pulsar_ctx *ctx) {
    return pulsar_client_configuration_get_memory_limit(ctx->client_conf);
}
const char* get_producer_name(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_producer_name(ctx->producer_conf);
}
const char* get_producer_topic(flb_out_pulsar_ctx *ctx) {
    return ctx->pulsar_producer_topic;
}      
int get_producer_send_timeout(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_send_timeout(ctx->producer_conf);
}
const char* get_producer_compression_type(flb_out_pulsar_ctx *ctx) {
    switch (pulsar_producer_configuration_get_compression_type(ctx->producer_conf))
    {
    case pulsar_CompressionNone:
        return "NONE";
    case pulsar_CompressionLZ4:
        return "LZ4";
    case pulsar_CompressionZLib:
        return "ZLIB";
    case pulsar_CompressionZSTD:
        return "ZSTD";
    case pulsar_CompressionSNAPPY:
        return "SNAPPY";
    default:
        return "";
    }
}
int64_t get_producer_initial_sequence_id(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_initial_sequence_id(ctx->producer_conf);
}
int get_producer_max_pending_messages(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_max_pending_messages(ctx->producer_conf);
}
int get_producer_max_pending_messages_across_partitions(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_max_pending_messages_across_partitions(ctx->producer_conf);
}
const char* get_producer_partitions_routing_mode(flb_out_pulsar_ctx *ctx) {
    switch (pulsar_producer_configuration_get_partitions_routing_mode(ctx->producer_conf))
    {
    case pulsar_UseSinglePartition:
        return "UseSingle";
    case pulsar_RoundRobinDistribution:
        return "RoundRobin";
    default:
        return "Custom";
    }
}
const char* get_producer_hashing_scheme(flb_out_pulsar_ctx *ctx) {
    switch (pulsar_producer_configuration_get_hashing_scheme(ctx->producer_conf))
    {
    case pulsar_JavaStringHash:
        return "JavaStringHash";
    case pulsar_Murmur3_32Hash:
        return "Murmur3_32Hash";
    case pulsar_BoostHash:
        return "BoostHash";
    default:
        return "";
    }
}
int get_producer_lazy_start_partitioned_producers(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_lazy_start_partitioned_producers(ctx->producer_conf);
}
const char* get_producer_block_if_queue_full(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_block_if_queue_full(ctx->producer_conf) ? "true" : "false";
}
const char* get_producer_batching_enabled(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_batching_enabled(ctx->producer_conf) ? "true" : "false";
}
uint32_t get_producer_batching_max_messages(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_batching_max_messages(ctx->producer_conf);
}
uint32_t get_producer_batching_max_allowed_size_in_bytes(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_batching_max_allowed_size_in_bytes(ctx->producer_conf);
}
uint32_t get_producer_batching_max_publish_delay_ms(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_configuration_get_batching_max_publish_delay_ms(ctx->producer_conf);
}
const char* get_producer_encryption_enabled(flb_out_pulsar_ctx *ctx) {
    return pulsar_producer_is_encryption_enabled(ctx->producer_conf) ? "true" : "false";
}
const char* get_producer_crypto_failure_action(flb_out_pulsar_ctx *ctx) {
    switch (pulsar_producer_configuration_get_crypto_failure_action(ctx->producer_conf))
    {
    case pulsar_ProducerSend:
        return "SEND";
    case pulsar_ProducerFail:
        return "FAIL";
    default:
        return "";
    }
}

// int context
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

    flb_out_pulsar_ctx *ctx = flb_calloc(1, sizeof(flb_out_pulsar_ctx));
    if (!ctx) {
        flb_errno();
        flb_plg_error(ins, "calloc pulsar context failed.");
        return NULL;
    }

    // init context
    ctx->ins = ins;
    ctx->pulsar_broker_url = NULL;
    ctx->pulsar_auth_token = NULL;
    ctx->pulsar_producer_topic = NULL;
    ctx->client = NULL;
    ctx->producer = NULL;
    ctx->authentication = NULL;
    ctx->client_conf = NULL;
    ctx->producer_conf = NULL;
    ctx->total_number = 0;
    ctx->failed_number = 0;
    ctx->success_number = 0;
    ctx->discarded_number = 0;
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
    if (ctx->pulsar_broker_url == NULL || ctx->pulsar_producer_topic == NULL) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "MUST be specify field '%s' and '%s'.", PULSAR_KEY_BROKER_URL, PULSAR_KEY_TOPIC_NAME);
        return NULL;
    }

    // init pulsar producer send function
    ctx->send_msg_func = (ctx->is_async ? pulsar_async_send : pulsar_send_msg);

    /*
     * config and create pulsar client
     */
    ctx->client_conf = pulsar_client_configuration_create();
    if (ctx->pulsar_auth_token && 0 < strlen(ctx->pulsar_auth_token)) {
        ctx->authentication = pulsar_authentication_token_create(ctx->pulsar_auth_token);
        pulsar_client_configuration_set_auth(ctx->client_conf, ctx->authentication);
    }

    pvalue = flb_output_get_property(PULSAR_KEY_MEMORY_LIMIT, ins);
    if (pvalue && PULSAR_DEFAULT_MEMORY_LIMIT < (memory_limit = atol(pvalue))) {
        pulsar_client_configuration_set_memory_limit(ctx->client_conf, memory_limit);
    }
    
    ctx->client = pulsar_client_create(ctx->pulsar_broker_url, ctx->client_conf);
    if (!ctx->client) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "create pulsar client failed !");
        return NULL;
    }
    
    /*
     * config and create pulsar producer
     */
    ctx->producer_conf = pulsar_producer_configuration_create();

    // set producerName
    pvalue = flb_output_get_property(PULSAR_KEY_PRODUCER_NAME, ins);
    if (pvalue) {
        pulsar_producer_configuration_set_producer_name(ctx->producer_conf, pvalue);
    }

    // set compressionType
    pvalue = flb_output_get_property(PULSAR_KEY_COMPRESSION_TYPE, ins);
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
            flb_plg_warn(ins, "unsupported pulsar %s: %s", PULSAR_KEY_COMPRESSION_TYPE, pvalue);
        }
    }

    // set sendTimeoutMs
    pvalue = flb_output_get_property(PULSAR_KEY_SEND_TIMEOUT, ins);
    if (pvalue && 0 < (send_timeout = atol(pvalue))) {
        pulsar_producer_configuration_set_send_timeout(ctx->producer_conf, send_timeout);
    }

    // set messageRoutingMode
    pvalue = flb_output_get_property(PULSAR_KEY_MESSAGE_ROUTING_MODE, ins);
    if (pvalue) {
        if (0 == strcasecmp("UseSingle", pvalue)) {
            pulsar_producer_configuration_set_partitions_routing_mode(ctx->producer_conf, pulsar_UseSinglePartition);
        } else if (0 == strcasecmp("RoundRobin", pvalue)) {
            pulsar_producer_configuration_set_partitions_routing_mode(ctx->producer_conf, pulsar_RoundRobinDistribution);
        } else if (0 == strcasecmp("Custom", pvalue)) {
            pulsar_producer_configuration_set_partitions_routing_mode(ctx->producer_conf, pulsar_CustomPartition);
        } else {
            flb_plg_warn(ins, "unsupported pulsar %s: %s", PULSAR_KEY_MESSAGE_ROUTING_MODE, pvalue);
        }
    }

    // set batching config
    pvalue = flb_output_get_property(PULSAR_KEY_BATCHING_ENABLED, ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_batching_enabled(ctx->producer_conf, false);
        }
    }
    pvalue = flb_output_get_property(PULSAR_KEY_BATCHING_MAX_MESSAGES, ins);
    if (pvalue && 0 < (batch_max_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_messages(ctx->producer_conf, batch_max_msg);
    }
    pvalue = flb_output_get_property(PULSAR_KEY_BATCHING_MAX_BYTES, ins);
    if (pvalue && 0 < (batch_max_bytes = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_allowed_size_in_bytes(ctx->producer_conf, batch_max_bytes);
    }
    pvalue = flb_output_get_property(PULSAR_KEY_BATCHING_MAX_DEPLY, ins);
    if (pvalue && 0 < (batch_max_delay = atol(pvalue))) {
        pulsar_producer_configuration_set_batching_max_publish_delay_ms(ctx->producer_conf, batch_max_delay);
    }

    // set blockIfQueueFull
    pvalue = flb_output_get_property(PULSAR_KEY_BLOCK_IF_QUEUE_FULL, ins);
    if (pvalue) {
        if (0 == strcasecmp("true", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, true);
        } else if (0 == strcasecmp("false", pvalue)) {
            pulsar_producer_configuration_set_block_if_queue_full(ctx->producer_conf, false);
        }
    }

    // set maxPendingMessages
    pvalue = flb_output_get_property(PULSAR_KEY_MAX_PENDING_MASSAGES, ins);
    if (pvalue && 0 < (max_pending_msg = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages(ctx->producer_conf, max_pending_msg);
    }

    // set maxPendingMessagesAcrossPartitions
    pvalue = flb_output_get_property(PULSAR_KEY_MAX_PENDING_MASSAGES_PARTITIONS, ins);
    if (pvalue && 0 < (max_pending_par = atol(pvalue))) {
        pulsar_producer_configuration_set_max_pending_messages_across_partitions(ctx->producer_conf, max_pending_par);
    }

    // set hashingScheme
    pvalue = flb_output_get_property(PULSAR_KEY_HASHING_SCHEMA, ins);
    if (pvalue) {
        if (0 == strcasecmp("Murmur3_32Hash", pvalue)) {
            pulsar_producer_configuration_set_hashing_scheme(ctx->producer_conf, pulsar_Murmur3_32Hash);
        } else if (0 == strcasecmp("BoostHash", pvalue)) {
            pulsar_producer_configuration_set_hashing_scheme(ctx->producer_conf, pulsar_BoostHash);
        } else if (0 == strcasecmp("JavaStringHash", pvalue)) {
            pulsar_producer_configuration_set_hashing_scheme(ctx->producer_conf, pulsar_JavaStringHash);
        } else {
            flb_plg_warn(ins, "unsupported pulsar %s: %s", PULSAR_KEY_HASHING_SCHEMA, pvalue);
        }
    }

    // set cryptoFailureAction
    pvalue = flb_output_get_property(PULSAR_KEY_CRYPTO_FAILURE_ACTION, ins);
    if (pvalue) {
        if (0 == strcasecmp("FAIL", pvalue)) {
            pulsar_producer_configuration_set_crypto_failure_action(ctx->producer_conf, pulsar_ProducerFail);
        } else if (0 == strcasecmp("SEND", pvalue)) {
            pulsar_producer_configuration_set_crypto_failure_action(ctx->producer_conf, pulsar_ProducerSend);
        } else {
            flb_plg_warn(ins, "unsupported pulsar %s: %s", PULSAR_KEY_CRYPTO_FAILURE_ACTION, pvalue);
        }
    }

    // create pulsar producer
    err = pulsar_client_create_producer(ctx->client, ctx->pulsar_producer_topic, ctx->producer_conf, &ctx->producer);
    if (err != pulsar_result_Ok) {
        flb_out_pulsar_destroy(ctx);
        flb_plg_error(ins, "Failed to create pulsar producer: %s\n", pulsar_result_str(err));
        return NULL;
    }
    
    // parse data schema
    pvalue = flb_output_get_property(OUTPUT_KEY_DATA_SCHEMA, ins);
    if (pvalue) {
        if (0 == strcasecmp("JSON", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_JSON;
        } else if (0 == strcasecmp("MSGPACK", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_MSGP;
        } else if (0 == strcasecmp("GELF", pvalue)) {
            ctx->data_schema = FLB_PULSAR_SCHEMA_GELF;
        } else {
            flb_plg_warn(ins, "unsupported output schema type: %s", pvalue);
        }
    }

    // print config
    flb_plg_info(ins, "fluent-bit pulsar output plugin config:\n"
        "    show progress interval:                 %u\n"
        "    output data schema:                     %s\n"
        "    pulsar client version:                  %u\n"
        "    is send message by async:               %s\n"
        "    pulsar url:                             %s\n"
        "    auth token:                             %s\n"
        "    memory limit:                           %"PRIu64"\n"
        "    producer name:                          %s\n"
        "    topic:                                  %s\n"
        "    send timeout:                           %d\n"
        "    compress type:                          %s\n"
        "    initial sequence id:                    %"PRId64"\n"
        "    max pending messages:                   %d\n"
        "    max pending messages across partitions: %d\n"
        "    partitions routing mode:                %s\n"
        "    hashing schema:                         %s\n"
        "    lazy start partitioned producers:       %d\n"
        "    block if queue full:                    %s\n"
        "    batching enabled:                       %s\n"
        "    batching max messages:                  %u\n"
        "    batching max bytes:                     %u\n"
        "    batching max publish delay:             %u\n"
        "    encryption enabled:                     %s\n"
        "    crypto failure action:                  %s\n",
        get_config_show_interval(ctx),
        get_config_output_schema(ctx),
        PULSAR_VERSION,
        get_msg_send_async(ctx),
        get_pulsar_url(ctx),
        get_pulsar_auth_token(ctx),
        get_pulsar_memory_limit(ctx),
        get_producer_name(ctx),
        get_producer_topic(ctx),
        get_producer_send_timeout(ctx),
        get_producer_compression_type(ctx),
        get_producer_initial_sequence_id(ctx),
        get_producer_max_pending_messages(ctx),
        get_producer_max_pending_messages_across_partitions(ctx),
        get_producer_partitions_routing_mode(ctx),
        get_producer_hashing_scheme(ctx),
        get_producer_lazy_start_partitioned_producers(ctx),
        get_producer_block_if_queue_full(ctx),
        get_producer_batching_enabled(ctx),
        get_producer_batching_max_messages(ctx),
        get_producer_batching_max_allowed_size_in_bytes(ctx),
        get_producer_batching_max_publish_delay_ms(ctx),
        get_producer_encryption_enabled(ctx),
        get_producer_crypto_failure_action(ctx));

    return ctx;
}

// close plugin
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

    if (ctx->pulsar_broker_url) {
        flb_free(ctx->pulsar_broker_url);
    }
    if (ctx->pulsar_auth_token) {
        flb_free(ctx->pulsar_auth_token);
    }
    if (ctx->pulsar_producer_topic) {
        flb_free(ctx->pulsar_producer_topic);
    }

    flb_free(ctx);
}
