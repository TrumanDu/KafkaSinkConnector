package com.aibibang.bigdata;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Truman.P.Du
 * @date May 29, 2019 10:52:25 AM
 * @version 1.0
 */
public class TopicInterceptor implements ProducerInterceptor<byte[], byte[]> {
	private String topic_rename_format = null;

	@Override
	public void configure(Map<String, ?> configs) {
		topic_rename_format = (String) configs.get(KafkaSinkConfig.TOPIC_RENAME_FORMAT);
	}

	@Override
	public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
		String topicString = KafkaSinkConfig.ganeratorTopicName(topic_rename_format, record.topic());
		ProducerRecord<byte[], byte[]> newRecord = new ProducerRecord<byte[], byte[]>(topicString, null, record.key(),
				record.value(), record.headers());
		return newRecord;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
}
