#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_time.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_utils.h>

#include <pulsar/c/client.h>

#include "pulsar_context.h"

bool flb_pulsar_output_msg(flb_out_pulsar_ctx *ctx, msgpack_object* map, struct flb_time *tm)
{
    char *out_buf;
    size_t out_size;
    
    msgpack_sbuffer mp_sbuf;
    msgpack_packer mp_pck;
    flb_sds_t s = NULL;

    flb_debug("in produce_message\n");
    if (flb_log_check(FLB_LOG_DEBUG))
        msgpack_object_print(stderr, *map);

    // Init temporal buffers
    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    msgpack_pack_map(&mp_pck, map->via.map.size);

    for (int i = 0; i < map->via.map.size; i++) {
        msgpack_pack_object(&mp_pck, map->via.map.ptr[i].key);
        msgpack_pack_object(&mp_pck, map->via.map.ptr[i].val);
    }

    // Parse to schema
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
                return false;
            }
            out_buf  = s;
            out_size = flb_sds_len(out_buf);
            break;
        }
    }

    bool ret = ctx->send_msg_func(ctx, out_buf, out_size);
    if (ret) {
        ++ctx->success_number;
        if (0 == ctx->success_number % ctx->show_interval) {
            flb_plg_info(ctx->ins, "output progress, total: %"PRIu64", success: %"PRIu64", failed: %"PRIu64", discarded: %"PRIu64", last msg: %s",
                ctx->total_number, ctx->success_number, ctx->failed_number, ctx->discarded_number, out_buf);
        }
    }
    if (s) {
        flb_sds_destroy(s);
    }

    msgpack_sbuffer_destroy(&mp_sbuf);
    return ret;
}

static int cb_pulsar_init(struct flb_output_instance *ins, struct flb_config *config, void *data)
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

static void cb_pulsar_flush(struct flb_event_chunk *event_chunk,
                            struct flb_output_flush *out_flush,
                            struct flb_input_instance *i_ins,
                            void *out_context,
                            struct flb_config *config)
{
    size_t off = 0;
    struct flb_time tms;
    msgpack_object *obj;
    msgpack_unpacked result;
    flb_out_pulsar_ctx *ctx = out_context;

    msgpack_unpacked_init(&result);
    while (MSGPACK_UNPACK_SUCCESS == msgpack_unpack_next(&result, event_chunk->data, event_chunk->size, &off)) {
        ++ctx->total_number;
        flb_time_pop_from_msgpack(&tms, &result, &obj);
        if (!flb_pulsar_output_msg(ctx, obj, &tms)) {
            ++ctx->failed_number;
        }
    }

    msgpack_unpacked_destroy(&result);
    FLB_OUTPUT_RETURN(FLB_OK);
}

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
        FLB_CONFIG_MAP_BOOL, "IsAsyncSend", "true", 0,
        FLB_TRUE, offsetof(flb_out_pulsar_ctx, is_async),
        "is sending a message asynchronous ?"
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
        FLB_CONFIG_MAP_STR, "MessageRoutingMode", (char *)NULL, 0, FLB_FALSE, 0,
        "pulsar producer message routing mode."
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
    .cb_flush     = cb_pulsar_flush,
    .cb_exit      = cb_pulsar_exit,
    .config_map   = config_map,
    .flags        = 0,
};
