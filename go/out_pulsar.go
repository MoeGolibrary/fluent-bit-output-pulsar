package main

import "C"
import "context"
import "log"
import "unsafe"
import "encoding/json"
import "github.com/fluent/fluent-bit-go/output"
import "github.com/apache/pulsar-client-go/pulsar"

var defaultMap = map[string]string{
    "PulsarBrokerUrl": "pulsar://localhost:6650",
    "Topic":           "persistent://moego/basic/test",
    "Token":           "",
}

var pulsarClient   pulsar.Client
var pulsarProducer pulsar.Producer

// init pulsar
func initPulsar(url string, token string, topic string) bool {
    var err error
    var opts pulsar.ClientOptions
    if token == "" {
        opts = pulsar.ClientOptions{ URL: url }
    } else {
        opts = pulsar.ClientOptions{
            URL:            url,
            Authentication: pulsar.NewAuthenticationToken(token),
        }
    }
    pulsarClient, err = pulsar.NewClient(opts)
    if err != nil {
        log.Printf("pulsar-go -> create pulsar client failed: %s, %v\n", url, err)
        return false
    }

    pulsarProducer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
        Topic:           topic,
        CompressionType: pulsar.LZ4,
    })
    if err != nil {
        log.Printf("pulsar-go -> create pulsar producer failed: %s, %v\n", topic, err)
        return false
    }

    return true
}

var msgTotalNumber, msgFailedNumber int = 0, 0

// send msg
func sendMsg(msg []byte) bool {
    _, err := pulsarProducer.Send(context.Background(), &pulsar.ProducerMessage{
        Payload: msg,
    })
    // log.Printf("pulsar-go -> output msg: %s\n", string(msg))
    if err != nil {
		msgFailedNumber++
        log.Printf("-> err: %v\n", err)
        return false
    }

	msgTotalNumber++
	if 0 == msgTotalNumber % 100 {
		log.Printf("pulsar-go -> progress: total: %d, failed: %d, last record: %s\n", msgTotalNumber, msgFailedNumber, string(msg))
	}

    return true
}

// exit pulsar
func exitPulsar() {
    if pulsarProducer != nil {
        pulsarProducer.Close()
    }
    if pulsarClient != nil {
        pulsarClient.Close()
    }

    log.Printf("pulsar-go -> pulsar shutdown ...")
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
    log.Printf("pulsar-go -> regisger pulsar go plugin\n")
    return output.FLBPluginRegister(ctx, "pulsar", "Output to Apache Pulsar")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
    url := getConfigKey(plugin, "PulsarBrokerUrl")
    topic := getConfigKey(plugin, "Topic")
    token := getConfigKey(plugin, "Token")
    log.Printf("pulsar-go -> PulsarBrokerUrl: %s, Topic: %s, Token: %s\n", url, topic, maskText(token, 8))

    if !initPulsar(url, token, topic) {
        return output.FLB_ERROR
    }

    log.Printf("pulsar-go -> connect to pulsar ok: %s, %s\n", url, topic)
    return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
    dec := output.NewDecoder(data, int(length))

    count := 0
    for {
        ret, _, record := output.GetRecord(dec)
        if ret != 0 {
            break
        }

        // log.Printf("pulsar-go -> original record: %v\n", record)
        formatted := translateData(record)
        // log.Printf("pulsar-go -> formatted data: %v\n", formatted)
        payload, err := json.Marshal(formatted)

        if err != nil {
            log.Printf("pulsar-go -> serialize record error: %v\n", err)
            return output.FLB_ERROR
        }

        if sendMsg(payload) {
            count++
        }
    }

    // log.Printf("pulsar-go -> output record: %d\n", count)
    return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
    exitPulsar()
    return output.FLB_OK
}

func getConfigKey(plugin unsafe.Pointer, key string) string {
    s := output.FLBPluginConfigKey(plugin, key)
    if len(s) == 0 {
        return defaultMap[key]
    } else {
        return s
    }
}

func translateData(data interface{}) interface{} {
    switch t := data.(type) {
    case nil:
        return nil
    case []byte:
        return string(t)
    case map[interface{}]interface{}:
        return translateMap(t)
    case []map[interface {}]interface {}:
        arr := make([]map[string]interface{}, len(t))
        for i, v := range t {
            arr[i] = translateMap(v)
        }
        return arr
    case []interface{}:
        arr := make([]interface{}, len(t))
        for i, v := range t {
            arr[i] = translateData(v)
        }
        return arr
    default:
        return t
    }
}

func translateMap(m map[interface{}]interface{}) map[string]interface{} {
    result := make(map[string]interface{})
    for k, v := range m {
        result[k.(string)] = translateData(v)
    }

    return result
}

func maskText(text string, n int) string {
    if n < len(text) {
        return text[:n] + "****"
    }

    return text
}

func main() {
}
