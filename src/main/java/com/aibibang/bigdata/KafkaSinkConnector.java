package com.aibibang.bigdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Truman.P.Du
 * @date May 28, 2019 1:57:14 PM
 * @version 1.0
 */
public class KafkaSinkConnector extends SinkConnector {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSinkConnector.class);
	Map<String, String> config;
	private AdminClient destAdminClient;
	private AdminClient sourceAdminClient;
	private KafkaSinkConfig kafkaSinkConfig;

	@Override
	public String version() {
		return Version.version();
	}

	@Override
	public ConfigDef config() {
		return KafkaSinkConfig.getConfigDef();
	}

	@Override
	public void start(Map<String, String> config) {
		logger.info("Connector starting");
		this.config = config;
		String topicFormatString = config.get(KafkaSinkConfig.TOPIC_RENAME_FORMAT);

		kafkaSinkConfig = new KafkaSinkConfig(config);
		destAdminClient = KafkaAdminClient.create(kafkaSinkConfig.getKafkaAdminClientProperties());
		sourceAdminClient = AdminClient.create(kafkaSinkConfig.getKafkaSourceAdminClientProperties());

		String[] array = config.get("topics").split(",");
		List<String> topics = new ArrayList<>();
		for (int i = 0; i < array.length; i++) {
			topics.add(array[i]);

		}
		Set<String> existTopics = new HashSet<>();
		try {
			existTopics.addAll(destAdminClient.listTopics().names().get());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		// 在目的集群创建topic
		topics.removeAll(existTopics);
		try {
			Map<String, TopicDescription> sourceTopicsMap = sourceAdminClient.describeTopics(topics).all().get();
			List<NewTopic> newTopics = new ArrayList<>();
			short replicationFactor = 2;
			sourceTopicsMap.forEach((k, v) -> {
				String topicName = KafkaSinkConfig.ganeratorTopicName(topicFormatString, v.name());
				logger.info("topicName:{} partition size:{}",topicName,v.partitions().size());
				NewTopic newTopic = new NewTopic(topicName, v.partitions().size(), replicationFactor);
				newTopics.add(newTopic);
			});
			destAdminClient.createTopics(newTopics);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		logger.info("Connector stopped.");
		sourceAdminClient.close();
		destAdminClient.close();
	}

	@Override
	public Class<? extends Task> taskClass() {
		return KafkaSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(config);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;

	}

}
