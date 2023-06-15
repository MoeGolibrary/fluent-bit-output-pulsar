[English](README.md) | 中文

# fluent-bit-output-pulsar
一个将采集到的数据发送到 pulsar 的 fluent-bit 输出插件。

* [Fluent Bit](https://docs.fluentbit.io/manual) 是一个高效、轻量级的，用于日志、指标、追踪的 Telemetry Agent。
* [Pulsar](https://pulsar.apache.org/docs/2.10.x/concepts-overview/) 是一个多租户、高性能的端到端的消息平台。

fluent-bit 是可插拔可扩展的，集成了许多输入/输出插件用于数据处理，但是官方内置的输出插件还不支持 pulsar，本项目旨在实现这一功能。

## 构建和安装
#### 前置条件
* 参见 [fluent-bit](https://docs.fluentbit.io/manual/installation/requirements) 和 [pulsar](https://pulsar.apache.org/api/cpp/2.10.x/) 仓库的构建要求，一般来说需要安装 GCC、CMake、Flex & Bison。
* 构建或下载 pulsar 客户端库
  * 如果你想自行构建 pulsar 客户端库，请参考：[Pulsar C++ client library
](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp#pulsar-c-client-library)
  * 或者，在 Linux 平台你可以直接从官方下载一个编译好的库文件：`libpulsar.so`，参见：[pulsar cpp library install](https://pulsar.apache.org/docs/client-libraries-cpp#install-dependencies)，但不论哪种方式，你最终都应该将 pulsar 的客户端库文件 `libpulsar.so` 安装到系统的 `/usr/lib` 目录中。
* 下一步你需要准备好两个环境变量：
  * **PULSAR_CPP_HEADERS**：pulsar 客户端库头文件的绝对路径。
  * **FLB_SOURCE**：fluent-bit 源代码的绝对路径。

  你可以 clone [fluent-bit](https://github.com/fluent/fluent-bit) 和 [pulsar](https://github.com/apache/pulsar) 仓库，它们都是开源的。例如，你可以将它们 clone 到本地目录 `/home/repo`，那么刚才的两个环境变量指定如下：
  ```shell
  export PULSAR_CPP_HEADERS=/home/repo/pulsar/pulsar-client-cpp/include
  export FLB_SOURCE=/home/repo/fluent-bit
  ```
#### 构建
```
# cd 到本仓库目录
mkdir build
cd build

cmake -DPULSAR_CPP_HEADERS=${PULSAR_CPP_HEADERS} -DFLB_SOURCE=${FLB_SOURCE} -DPLUGIN_NAME=out_pulsar ..
make
```
现在你应该可以在 `build` 目录中看到编译好的库文件 `flb-out_pulsar.so`，启动 fluent-bit 并且指定生成的库文件和自己的配置：
```shell
bin/fluent-bit -e /path/to/flb-out_pulsar.so -c /path/to/fluent-bit.conf
```
#### 打包
如果你需要自行构建 docker 镜像，你需要将生成的库文件 `flb-out_pulsar.so` 和 pulsar 客户端库文件 `libpulsar.so` 一起打包进 fluent-bit，这里有一个 Dockerfile 参考：[Dockerfile](Dockerfile)

## 配置介绍
本插件支持大多数的 pulsar 客户端参数，并且参数名与 pulsar 官方一致，参见：[Pulsar Producer configuration](https://pulsar.apache.org/reference/#/3.0.x/client/client-configuration-producer)

示例:
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
部分字段描述如下，其他字段参见：[Pulsar Producer configuration](https://pulsar.apache.org/reference/#/3.0.x/client/client-configuration-producer)

| Field | Type | Description                                  |
| --- | --- |----------------------------------------------|
| showInterval | int | 输出间隔，每采集 `showInterval` 条数据打印一次进度            |
| dataSchema | string | 发送数据的 schema，可以指定的值有：`JSON`、`MSGPACK`、`GELF` |
| pulsarBrokerUrl | string | 指定 pulsar broker 或 proxy 的 url 地址            |
| pulsarAuthToken | string | pulsar 的连接授权 token                           |
| isAsyncSend | bool | 指定是否采用异步发送消息                                 |

### 插件版本依赖
| plugin: fluent-bit-output-pulsar | fluent-bit | pulsar-client |
|----------------------------------|------------|---------------|
| 1.1.0                            | v2.1.4      | v2.10.2       |
| 1.0.0                            | v1.9.4      | v2.10.0       |

## Q & A
* 在构建过程中可能发现某些头文件找不到，例如 `fluent-bit/flb_info.h` 或 `pulsar/version.h`。
    
    这是因为 fluent-bit 和 pulsar 项目中的一些头文件是以内联方式定义的，这种头文件需要在构建过程中生成，因此只需要进入到对应项目的目录，参照官方构建介绍执行 CMake 命令即可生成缺失的头文件。
