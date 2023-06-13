# fluent-bit-output-pulsar
A fluent-bit output plugin to output the collected data to pulsar.

[Fluent Bit](https://docs.fluentbit.io/manual) is a Fast and Lightweight Telemetry Agent for Logs, Metrics, and Traces.

[Pulsar](https://pulsar.apache.org/docs/2.10.x/concepts-overview/) is a multi-tenant, high-performance solution for server-to-server messaging.

fluent-bit is pluggable and integrates many plugins for data input/output processing. However, the official built-in output plugin does not support pulsar yet. This project aims to realize this function.

## Build and Install
#### Preconditions
* Build or Download pulsar client library
  * If you want to build pulsar library by yourself, please see: [Pulsar C++ client library
](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp#pulsar-c-client-library)
  * OR, In Linux, You can directly download the official compiled library: libpulsar.so, please see: [pulsar cpp library install](https://pulsar.apache.org/docs/client-libraries-cpp#install-dependencies), but either way, you should eventually install the pulsar client library file `libpulsar.so` to the `/usr/lib` directory in your system.
* Next you need to specify 2 environment variables for CMake:
  * **PULSAR_CPP_HEADERS**: absolute path to pulsar cpp library header files.
  * **FLB_SOURCE**: absolute path to source code of Fluent Bit.

  You can clone the [fluent-bit](https://github.com/fluent/fluent-bit) and [pulsar](https://github.com/apache/pulsar) repositories, both of which are open source. For example, assuming you have cloned them to the local directory `/home/repo`, the above two environment variables can be specified as:
  ```
  export PULSAR_CPP_HEADERS=/home/repo/pulsar/pulsar-client-cpp/include
  export FLB_SOURCE=/home/repo/fluent-bit
  ```
#### Build
```
# cd to this repository directory
mkdir build
cd build

cmake -DPULSAR_CPP_HEADERS=${PULSAR_CPP_HEADERS} -DFLB_SOURCE=${FLB_SOURCE} -DPLUGIN_NAME=out_pulsar ..
make
```
Now you can see the generated `flb-out_pulsar.so` library file in the `build` directory, start fluent-bit and specify the library file path to use the pulsar output plugin.
```
bin/fluent-bit -e /path/to/flb-out_pulsar.so -c /path/to/config
```
#### Package
If you need to package the image yourself, you need to package the generated plugin library file `flb-out_pulsar.so` and the pulsar client library file `libpulsar.so` together into fluent-bit, here is a Dockerfile reference: [Dockerfile](Dockerfile).

## Configuration Instructions
This plugin supports most of the pulsar client parameters, and the parameter field names are consistent with the pulsar official, see: [Pulsar Producer configuration](https://pulsar.apache.org/reference/#/3.0.x/client/client-configuration-producer)

Here is an example configuration:
```conf
# fluent-bit.conf
[INPUT]
    Name                tail
    Tag                 application.*
    Alias               tail-app
    Path                <some path>
    ...

[OUTPUT]
    Name                          pulsar
    Match                         application.*
    showInterval                  200
    dataSchema                    JSON
    pulsarBrokerUrl               pulsar://pulsar-broker.pulsar:6650
    pulsarAuthToken               <your token>
    isAsyncSend                   true
    topicName                     persistent://tenant/namespace/topic
    compressionType               LZ4
    blockIfQueueFull              true
    batchingEnabled               true
    batchingMaxMessages           1000
    batchingMaxBytes              4194304
    batchingMaxPublishDelayMicros 20000
    messageRoutingMode            RoundRobin
```
Some fields are described as follows, other fields refer to [Pulsar Producer configuration](https://pulsar.apache.org/reference/#/3.0.x/client/client-configuration-producer)

| Field | Type | Description                                                                       |
| --- | --- |-----------------------------------------------------------------------------------|
| showInterval | int | Output interval of collected data, print the progress every `showInterval`        |
| dataSchema | string | The schema of the data to be sent, currently supports: `JSON`, `MSGPACK`, `GELF`. |
| pulsarBrokerUrl | string | The pulsar broker or proxy url.                                                   |
| pulsarAuthToken | string | The pulsar authentication token.                                                  |
| isAsyncSend | bool | Whether to send asynchronously.                                                   |

### Version Dependencies
| plugin: fluent-bit-output-pulsar | fluent-bit | pulsar-client |
|----------------------------------|------------|---------------|
| 1.1.0                            | v2.1.4      | v2.10.4      |
| 1.0.0                            | v1.9.4      | v2.10.0      |

## Q & A
* It is possible to encounter some header files that cannot be found during the build process, such as `fluent-bit/flb_info.h` or `pulsar/version.h`.
    
    This is because some header files in the `fluent-bit` and `pulsar` projects are defined inline and generated during the build process.
    Just switch Go to the corresponding project and execute the cmake command according to their official guidance process to generate the required header files.
