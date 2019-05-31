package com.aibibang.bigdata;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Truman.P.Du
 * @date May 28, 2019 1:59:25 PM
 * @version 1.0
 */
public class KafkaSinkTask extends SinkTask {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSinkTask.class);
	KafkaProducer<byte[], byte[]> kafkaProducer;
	KafkaSinkConfig kafkaSinkConfig;
	String topicFormat;

	@Override
	public String version() {
		return Version.version();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		sinkRecords.forEach(sinkRecord -> {
			@SuppressWarnings("unchecked")
			ProducerRecord<byte[], byte[]> record = new ProducerRecord(sinkRecord.topic(), null, sinkRecord.key(),
					sinkRecord.value(), sinkRecord.headers());
			kafkaProducer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null) {
						logger.error("migrate data error.", exception);
					}

				}
			});
		});

	}

	@Override
	public void start(Map<String, String> configs) {
		kafkaSinkConfig = new KafkaSinkConfig(configs);
		topicFormat = configs.get(KafkaSinkConfig.TOPIC_RENAME_FORMAT);
		kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaSinkConfig.getKafkaProducerProperties());
	}

	@Override
	public void stop() {
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
		logger.info("{}: task has been stopped", this);
	}
}
