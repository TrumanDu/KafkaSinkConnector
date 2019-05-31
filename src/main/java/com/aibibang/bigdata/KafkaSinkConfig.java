package com.aibibang.bigdata;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * @author Truman.P.Du
 * @date May 28, 2019 3:54:06 PM
 * @version 1.0
 */
public class KafkaSinkConfig {
	private static String DEST_BOOTSTRAP_SERVERS = "dest.bootstrap.servers";
	private static String SOURCE_BOOTSTRAP_SERVERS = "source.bootstrap.servers";
	public static String TOPIC_RENAME_FORMAT = "topic.rename.format";
	private Map<String, String> configs;

	public KafkaSinkConfig(Map<String, String> configs) {
		this.configs = configs;
		if (!configs.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		}

		if (!configs.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
			configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		}

		if (!configs.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		}

		if (!configs.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		}
	}

	public Properties getKafkaProducerProperties() {
		Properties kafkaProducerProperties = new Properties();
		kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(DEST_BOOTSTRAP_SERVERS));
		if (configs.containsKey(TOPIC_RENAME_FORMAT)) {
			kafkaProducerProperties.put(TOPIC_RENAME_FORMAT, configs.get(TOPIC_RENAME_FORMAT));
		}

		kafkaProducerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TopicInterceptor.class.getName());
		kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		return kafkaProducerProperties;
	}

	public Properties getKafkaAdminClientProperties() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(DEST_BOOTSTRAP_SERVERS));
		return properties;
	}

	public Properties getKafkaSourceAdminClientProperties() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configs.get(SOURCE_BOOTSTRAP_SERVERS));
		return properties;
	}

	public static ConfigDef getConfigDef() {
		return new ConfigDef()
				.define(DEST_BOOTSTRAP_SERVERS, Type.STRING, Importance.HIGH,
						"Mandatory. Comma separated list of boostrap servers for the dest Kafka cluster.")
				.define(SOURCE_BOOTSTRAP_SERVERS, Type.STRING, Importance.HIGH,
						"Mandatory. Comma separated list of boostrap servers for the source Kafka cluster.")
				.define(SinkConnector.TOPICS_CONFIG, Type.STRING, Importance.HIGH, "topics");
	}

	public static String ganeratorTopicName(String format, String topic) {
		if (format == null) {
			return topic;
		}
		int start = format.indexOf("$");
		int end = format.indexOf("}");

		format = format.substring(0, start) + "%s" + format.substring(end + 1, format.length());

		return String.format(format, topic);
	}
}
