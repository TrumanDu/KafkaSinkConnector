# KafkaSinkConnector

## 简介

KafkaSinkConnector 支持迁移数据从kafka到kafka

## Common Options

Configuration | Parameter | Example Description
---|---|---
dest.bootstrap.servers|dest.broker1:9092,dest.broker2:9092|Mandatory. Comma separated list of boostrap servers for the dest Kafka cluster
source.bootstrap.servers|source.broker1:9092,source.broker2:9092|Mandatory. Comma separated list of boostrap servers for the source Kafka cluster
topic.rename.format | ${topic}.replica | dest topic name
topic.whitelist | topic, topic-prefix* | Java regular expression to match topics to mirror. For convenience, comma (',') is interpreted as the regex-choice symbol ('|').
topics| topic | List of topics to consume, separated by commas

## Demo

```
{
    "name": "kafka-sink-connector",
    "config": {
        "connector.class": "com.aibibang.bigdata.KafkaSinkConnector",
        "dest.bootstrap.servers": "192.168.0.101:8092",
        "source.bootstrap.servers": "192.168.0.102:8092",
        "topic.rename.format": "${topic}",
        "topics": "truman_test_connect,",
         "tasks.max": "2",
        "key.converter":"org.apache.kafka.connect.converters.ByteArrayConverter",
        "value.converter":"org.apache.kafka.connect.converters.ByteArrayConverter"
    }
}
```