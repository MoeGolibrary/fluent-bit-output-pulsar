FROM fluent/fluent-bit:1.9.3

COPY ./output/out_pulsar.so /fluent-bit/bin/

CMD ["/fluent-bit/bin/fluent-bit", "-e", "/fluent-bit/bin/out_pulsar.so", "-c", "/fluent-bit/etc/fluent-bit.conf"]
