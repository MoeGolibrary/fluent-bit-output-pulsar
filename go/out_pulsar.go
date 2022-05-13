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
        log.Printf("go-pulsar -> create pulsar client failed: %s, %v\n", url, err)
        return false
    }

    pulsarProducer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
        Topic:           topic,
        CompressionType: pulsar.LZ4,
    })
    if err != nil {
        log.Printf("go-pulsar -> create pulsar producer failed: %s, %v\n", topic, err)
        return false
    }

    return true
}

var msgTotalNumber, msgSentNumber, msgSuccessNumber, msgFailedNumber int = 0, 0, 0, 0

// send msg
func sendMsg(msg []byte) {
    _, err := pulsarProducer.Send(context.Background(), &pulsar.ProducerMessage{
        Payload: msg,
    })
    // log.Printf("go-pulsar -> output msg: %s\n", string(msg))
    if err != nil {
        log.Printf("go-pulsar -> send msg error: %v\n", err)
    } else {
	    msgSentNumber++
	    if 0 == msgSentNumber % 200 {
            err = pulsarProducer.Flush()
            if err == nil {
                msgSuccessNumber++
                log.Printf("go-pulsar -> flush msg ok !\n")
            } else {
                msgFailedNumber++
                log.Printf("go-pulsar -> flush msg error: %v\n", err)
            }
            log.Printf("go-pulsar -> progress: total: %d, sent: %d, success: %d, failed: %d, last record: %s\n", msgTotalNumber, msgSentNumber, msgSuccessNumber, msgFailedNumber, string(msg))
        }
	}
}

// exit pulsar
func exitPulsar() {
    if pulsarProducer != nil {
        pulsarProducer.Close()
    }
    if pulsarClient != nil {
        pulsarClient.Close()
    }

    log.Printf("go-pulsar -> pulsar shutdown ...")
}

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
    log.Printf("go-pulsar -> regisger pulsar go plugin\n")
    return output.FLBPluginRegister(ctx, "pulsar", "Output to Apache Pulsar")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
    url := getConfigKey(plugin, "PulsarBrokerUrl")
    topic := getConfigKey(plugin, "Topic")
    token := getConfigKey(plugin, "Token")
    log.Printf("go-pulsar -> PulsarBrokerUrl: %s, Topic: %s, Token: %s\n", url, topic, maskText(token, 8))

    if !initPulsar(url, token, topic) {
        return output.FLB_ERROR
    }

    log.Printf("go-pulsar -> connect to pulsar ok: %s, %s\n", url, topic)
    return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
    dec := output.NewDecoder(data, int(length))

    for {
        ret, _, record := output.GetRecord(dec)
        if ret != 0 {
            break
        }

		msgTotalNumber++
		
        // log.Printf("go-pulsar -> original record: %v\n", record)
        formatted := translateData(record)
        // log.Printf("go-pulsar -> formatted data: %v\n", formatted)
        payload, err := json.Marshal(formatted)

        if err != nil {
			msgFailedNumber++
            log.Printf("go-pulsar -> serialize record error: %v\n", err)
            // return output.FLB_ERROR
        } else {
			sendMsg(payload)
		}
    }

    // log.Printf("go-pulsar -> output record: %d\n", count)
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
