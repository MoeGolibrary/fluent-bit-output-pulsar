FROM fluent/fluent-bit:2.1.4

COPY ./build/flb-out_pulsar.so /fluent-bit/bin/
COPY ./build/libpulsar.so.2.10.2 /fluent-bit/bin/

# CMD ["/fluent-bit/bin/fluent-bit", "-e", "/fluent-bit/bin/flb-out_pulsar.so", "-c", "/fluent-bit/etc/fluent-bit.conf"]
