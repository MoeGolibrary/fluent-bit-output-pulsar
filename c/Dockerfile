FROM fluent/fluent-bit:1.9.3

COPY ./build/flb-out_pulsar.so /fluent-bit/bin/
COPY ./build/libpulsar.so /fluent-bit/bin/
COPY ./build/libpulsar.so.2.9.2 /fluent-bit/bin/

# CMD ["/fluent-bit/bin/fluent-bit", "-e", "/fluent-bit/bin/flb-out_pulsar.so", "-c", "/fluent-bit/etc/fluent-bit.conf"]
