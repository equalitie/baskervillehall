package main

import (
    "fmt"
    "os"
    "os/signal"
    "syscall"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
    conf := &kafka.ConfigMap{
        "bootstrap.servers": "kafkab0.prod.deflect.network:9094,kafkab1.prod.deflect.network:9094,kafkab2.prod.deflect.network:9094",
        "security.protocol": "SSL",

        "ssl.ca.location":        "caroot.pem",
        "ssl.certificate.location": "certificate.pem",
        "ssl.key.location":         "key.pem",

        "ssl.endpoint.identification.algorithm": "none",

        "group.id":           "go-consumer-direct", // Required even for Assign()
        "enable.auto.commit": false,

        "fetch.min.bytes":           "1048576",  // 1MB - wait for batch
        "fetch.wait.max.ms":         "500",      // max wait 500ms, then return whatever is there
        "fetch.max.bytes":           "10485760", // 10MB  max fetch size
        "socket.receive.buffer.bytes": "16777216", // 16MB receive buffer

        "socket.keepalive.enable": true,
        "reconnect.backoff.ms":    500,
        "reconnect.backoff.max.ms": 5000,
    }

    c, err := kafka.NewConsumer(conf)
    if err != nil {
        panic(err)
    }
    defer c.Close()

    topic := "logstash_deflect.log"

    partition := int32(1)

    err = c.Assign([]kafka.TopicPartition{
        {Topic: &topic, Partition: partition, Offset: kafka.OffsetEnd},
    })
    if err != nil {
        panic(err)
    }

    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    fmt.Printf("Consumer started for partition %d...\n", partition)

    run := true
    messageCount := uint64(0)

    for run {
        select {
        case sig := <-sigchan:
            fmt.Printf("Caught signal %v: terminating (processed %d messages)\n", sig, messageCount)
            run = false

        default:
            ev := c.Poll(500)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                messageCount++
                if e.TopicPartition.Partition != partition {
                    fmt.Printf("WARNING: Got message from unexpected partition %d (expected %d)\n",
                        e.TopicPartition.Partition, partition)
                    continue
                }
                fmt.Printf("Partition:%d Offset:%d (#%d) Value: %.100s...\n",
                    e.TopicPartition.Partition, e.TopicPartition.Offset, messageCount, string(e.Value))

            case kafka.Error:
                fmt.Fprintf(os.Stderr, "Error: %v\n", e)
                if e.Code() == kafka.ErrAllBrokersDown {
                    run = false
                }
            }
        }
    }
}
