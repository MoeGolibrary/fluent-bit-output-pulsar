
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_utils.h>

#include <pulsar/c/client.h>
#include <stdio.h>

#include "pulsar_context.h"

#define PULSAR_MSG_BUFFER_SIZE (1 << 23)

bool flb_pulsar_send_msg2(flb_out_pulsar_ctx *ctx, msgpack_object* map, struct flb_time *tm) {

    char *out_buf;
    size_t out_size;
    flb_sds_t s = NULL;
    
    msgpack_sbuffer mp_sbuf;
    msgpack_packer mp_pck;

    flb_debug("in produce_message\n");
    if (flb_log_check(FLB_LOG_DEBUG))
        msgpack_object_print(stderr, *map);

    /* Init temporal buffers */
    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    size = map->via.map.size;
    msgpack_pack_map(&mp_pck, size);

    for (i = 0; i < map->via.map.size; i++) {
        key = map->via.map.ptr[i].key;
        val = map->via.map.ptr[i].val;
        msgpack_pack_object(&mp_pck, key);
        msgpack_pack_object(&mp_pck, val);
    }

    switch (ctx->data_schema)
    {
    case FLB_PULSAR_SCHEMA_MSGP:
        {
            out_buf = mp_sbuf.data;
            out_size = mp_sbuf.size;
            break;
        }
    case FLB_PULSAR_SCHEMA_JSON:
    default:
        {
            s = flb_msgpack_raw_to_json_sds(mp_sbuf.data, mp_sbuf.size);
            if (!s) {
                flb_plg_error(ctx->ins, "error encoding to JSON");
                msgpack_sbuffer_destroy(&mp_sbuf);
                return FLB_ERROR;
            }
            out_buf  = s;
            out_size = flb_sds_len(out_buf);
            break;
        }
    }

    flb_plg_debug(ctx->ins, "-------> output size: %d, msg: %s", out_size, out_buf);

    if (!s) {
        flb_sds_destroy(s);
    }

    msgpack_sbuffer_destroy(&mp_sbuf);
    return FLB_OK;
}

bool flb_pulsar_send_msg1(flb_out_pulsar_ctx *ctx, msgpack_object* obj)
{
    pulsar_result err;

    char buf[PULSAR_MSG_BUFFER_SIZE] = { 0 };
    // int flb_msgpack_to_json(char *json_str, size_t str_len, const msgpack_object *obj);
    int len = flb_msgpack_to_json(buf, PULSAR_MSG_BUFFER_SIZE, obj);
    // int len = msgpack_object_print_buffer(buf, PULSAR_MSG_BUFFER_SIZE, *obj);
    flb_plg_info(ctx->ins, "=====>>>>>>> debug send, len: %d, buf: %s", len, buf);
    if (len < 1) {
        return false;
    }

    flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 04");
    pulsar_message_t* message = pulsar_message_create();
    pulsar_message_set_content(message, buf, len);

    flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 05");
    err = pulsar_producer_send(ctx->producer, message);
    pulsar_message_free(message);
    flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 06");

    if (err == pulsar_result_Ok) {
        ++ctx->success_number;
    } else {
        ++ctx->failed_number;
        flb_plg_error(ctx->ins, "Failed to publish message: %s", pulsar_result_str(err));
    }
    
    ++ctx->total_number;
    if (0 == ctx->total_number % ctx->show_interval) {
        flb_plg_info(ctx->ins, "publish progress: total: %d, success: %d, failed: %d, last msg: %s",
            ctx->total_number, ctx->success_number, ctx->failed_number, buf);
    }
    flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 07");

    return (err == pulsar_result_Ok);
}

static int cb_pulsar_init(struct flb_output_instance *ins,
                          struct flb_config *config, void *data)
{
    // create output context
    flb_out_pulsar_ctx *ctx = flb_out_pulsar_create(ins, config);
    if (!ctx) {
        flb_plg_error(ins, "initialize pulsar context failed.");
        return -1;
    }

    // set global context
    flb_output_set_context(ins, ctx);
    return 0;
}

static int pulsar_print_fluent_record(size_t cnt, msgpack_unpacked result)
{
    msgpack_object o;
    msgpack_object *obj;
    msgpack_object root;
    struct flb_time tms;

    const size_t len = (1<<22);
    char buf[1 << 22] = { 0 };

    root = result.data;
    printf("[%d] debug =====>>>>>>> root.type: %d\n", __LINE__, root.type);

    if (root.type != MSGPACK_OBJECT_ARRAY) {
        return -1;
    }

    /* decode expected timestamp only (integer, float or ext) */
    o = root.via.array.ptr[0];
    printf("[%d] debug =====>>>>>>> o.type: %d\n", __LINE__, o.type);
    if (o.type != MSGPACK_OBJECT_POSITIVE_INTEGER &&
        o.type != MSGPACK_OBJECT_FLOAT &&
        o.type != MSGPACK_OBJECT_EXT) {
        return -1;
    }

    /* This is a Fluent Bit record, just do the proper unpacking/printing */
    flb_time_pop_from_msgpack(&tms, &result, &obj);

    printf("[%d] debug =====>>>>>>> \n", __LINE__);

    fprintf(stdout, "[%zd] [%"PRIu32".%09lu \n", cnt,
            (uint32_t) tms.tm.tv_sec, tms.tm.tv_nsec);
    // msgpack_object_print(stdout, *obj);
    int ret = msgpack_object_print_buffer(buf, len, *obj);
    printf("[%d] debug =====>>>>>>> %d/%d, buf: %s", __LINE__, ret, len, buf);
    // fprintf(stdout, "]\n");

    return 0;
}

void flb_pulsasr_print(const char *data, size_t bytes)
{
    int ret;
    msgpack_unpacked result;
    size_t off = 0, cnt = 0;

    msgpack_unpacked_init(&result);
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        /* Check if we are processing an internal Fluent Bit record */
        ret = pulsar_print_fluent_record(cnt, result);
        printf("[%d] debug =====>>>>>>> ret: %d\n", __LINE__, ret);
        if (ret == 0) {
            continue;
        }

        printf("[%d] debug =====>>>>>>> cnt: %d\n", __LINE__, cnt);
        printf("[%zd] ", cnt++);
        msgpack_object_print(stdout, result.data);
        printf("[%d] debug =====>>>>>>> cnt: %d\n", __LINE__, cnt);
        printf("\n");
    }
    msgpack_unpacked_destroy(&result);
}

static void cb_stdout_flush(struct flb_event_chunk *event_chunk,
                           struct flb_output_flush *out_flush,
                           struct flb_input_instance *i_ins,
                           void *out_context,
                           struct flb_config *config)
{
    flb_out_pulsar_ctx *ctx = out_context;
    flb_plg_info(ctx->ins, "=====>>>>>>> debug data start\n");
    flb_pulsasr_print(event_chunk->data, event_chunk->size);
    flb_plg_info(ctx->ins, "=====>>>>>>> debug data end\n");
    FLB_OUTPUT_RETURN(FLB_OK);
}
/*
static void cb_pulsar_flush(struct flb_event_chunk *event_chunk,
                            struct flb_output_flush *out_flush,
                            struct flb_input_instance *i_ins,
                            void *out_context,
                            struct flb_config *config)
{
    int ret;
    size_t off = 0;
    struct flb_time tms;
    msgpack_object *obj;
    msgpack_unpacked result;
    flb_out_pulsar_ctx *ctx = out_context;

    msgpack_unpacked_init(&result);
    while (MSGPACK_UNPACK_SUCCESS == msgpack_unpack_next(&result, event_chunk->data, event_chunk->size, &off)) {
        flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 01");
        flb_time_pop_from_msgpack(&tms, &result, &obj);
        flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 02");
        // msgpack_object_print(stdout, *obj);
        flb_plg_info(ctx->ins, "=====>>>>>>> debug send: 03");
        if (!flb_pulsar_send_msg(ctx, obj)) {
            flb_plg_error(ctx->ins, "pulsar send msg failed.");
        }
    }

    msgpack_unpacked_destroy(&result);
    FLB_OUTPUT_RETURN(FLB_OK);
}
*/
static int cb_pulsar_exit(void *data, struct flb_config *config)
{
    flb_out_pulsar_ctx *ctx = data;
    flb_plg_info(ctx->ins, "exit pulsar ok!");
    flb_out_pulsar_destroy(ctx);

    return 0;
}

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {
        FLB_CONFIG_MAP_STR, "PulsarUrl", (char *)NULL, 0,
        FLB_TRUE, offsetof(flb_out_pulsar_ctx, url),
        "pulsar broker or proxy url."
    },
    {
        FLB_CONFIG_MAP_STR, "Token", (char *)NULL, 0,
        FLB_TRUE, offsetof(flb_out_pulsar_ctx, token),
        "pulsar authentication token."
    },
    {
        FLB_CONFIG_MAP_STR, "Topic", (char *)NULL, 0,
        FLB_TRUE, offsetof(flb_out_pulsar_ctx, topic),
        "pulsar producer topic."
    },
    {
        FLB_CONFIG_MAP_INT, "ShowInterval", "200", 0,
        FLB_TRUE, offsetof(flb_out_pulsar_ctx, show_interval),
        "show progress interval number."
    },
    {
        FLB_CONFIG_MAP_INT, "DataSchema", "json", 0, FLB_FALSE, 0,
        "output data schema: json, msgpack, gelf."
    },
    {
        FLB_CONFIG_MAP_INT, "MemoryLimit", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar client memory limit."
    },
    {
        FLB_CONFIG_MAP_STR, "ProducerName", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer name."
    },
    {
        FLB_CONFIG_MAP_STR, "CompressType", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer compress type."
    },
    {
        FLB_CONFIG_MAP_INT, "SendTimeout", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer send msg timeout in milliseconds."
    },
    {
        FLB_CONFIG_MAP_BOOL, "BatchingEnabled", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer enable batch sending."
    },
    {
        FLB_CONFIG_MAP_INT, "BatchingMaxMessages", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer max number of batch sending."
    },
    {
        FLB_CONFIG_MAP_INT, "BatchingMaxBytes", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer max bytes of batch sending."
    },
    {
        FLB_CONFIG_MAP_INT, "BatchingMaxPublishDelay", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer max delay of batch sending in milliseconds."
    },
    {
        FLB_CONFIG_MAP_BOOL, "BlockIfQueueFull", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer enable batch block if queue full."
    },
    {
        FLB_CONFIG_MAP_INT, "MaxPendingMessages", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer: number of max pending messages."
    },
    {
        FLB_CONFIG_MAP_INT, "MaxPendingMessagesAcrossPartitions", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer: number of max pending messages across all the partitions."
    },
    /* EOF */
    {0}
};

struct flb_output_plugin out_pulsar_plugin = {
    .name         = "pulsar",
    .description  = "Push events to Pulsar",
    .cb_init      = cb_pulsar_init,
    // .cb_flush     = cb_pulsar_flush,
    .cb_flush     = cb_stdout_flush,
    .cb_exit      = cb_pulsar_exit,
    .config_map   = config_map,
    .flags        = 0,
};
