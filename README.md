# fluent-bit-output-pulsar

**fluent-bit output plugin for C**

### build and install

* Build or Download pulsar library
  * If you want to build pulsar library by yourself, please see: [Pulsar C++ client library
](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp#pulsar-c-client-library)
  * OR, In Linux, You can directly download the official compiled library: libpulsar.so, please see: [pulsar cpp library install](https://pulsar.apache.org/docs/client-libraries-cpp#install-dependencies)
* Next we need to specify 2 environment variables for CMake:
  * **PULSAR_CPP_HEADERS**: absolute path to pulsar cpp library header files.
  * **FLB_SOURCE**: absolute path to source code of Fluent Bit.

  You can clone the [fluent-bit](https://github.com/fluent/fluent-bit) and [pulsar](https://github.com/apache/pulsar) repositories, both of which are open source. For example, assuming you have cloned them to the local directory `/home/repo`, the above two environment variables can be specified as:
  ```
  PULSAR_CPP_HEADERS=/home/repo/pulsar/pulsar-client-cpp/include
  FLB_SOURCE=/home/repo/fluent-bit
  ```
* Run cmake and make
  ```
  mkdir build
  cd build

  cmake -DPULSAR_CPP_HEADERS=/path/to/pulsar -DFLB_SOURCE=/path/to/fluent-bit -DPLUGIN_NAME=out_pulsar ../
  make
  ```
* Now you can see the generated `flb-out_pulsar.so` library file in the build directory, start fluent-bit and specify the library file path to use the pulsar output plugin.
  ```
  bin/fluent-bit -e /path/to/flb-out_pulsar.so -c /path/to/config
  ```
