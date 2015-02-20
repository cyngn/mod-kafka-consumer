/*
 * Copyright 2015 Cyanogen Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import org.vertx.java.core.Future;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Module to listen for kafka messages and republish them on the vertx event bus
 *
 * to build:
 * ./gradlew clean fatjar
 *
 * to run (you generally will just deploy this module in your app but this is handy for testing):
 *  java -jar build/libs/mod-kafka-consumer-1.0.0-final-SNAPSHOT-fat.jar -conf conf.json
 *
 * NOTE: to run locally
 *  1) you need a zookeeper instance
 *  2) a kafka broker
 *  3) a registered topic with kafka
 *  4) something producing messages and sending them in to kafka
 *
 * i.e. from a kafka install directory
 *  1) bin/zookeeper-server-start.sh config/zookeeper.properties
 *  2) bin/kafka-server-start.sh config/server.properties
 *  3) bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic [yourTestTopic]
 *  4) bin/kafka-console-producer.sh --broker-list localhost:9092 --topic [yourTestTopic]
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 1/28/15
 */
public class MessageConsumer extends Verticle {
    private ConsumerConnector consumer;
    private ExecutorService threadPool;
    public static String EVENTBUS_DEFAULT_ADDRESS = "kafka.message";
    private String busAddress;
    private Logger logger;


    @Override
    public void start(final Future<Void> startedResult) {
        logger = container.logger();
        try {
            JsonObject config = container.config();

            Properties consumerConfig = populateKafkaConfig(config);

            busAddress = config.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);

            int workersPerTopic = config.getInteger(ConfigConstants.WORKERS_PER_TOPIC, 1);
            JsonArray topics = config.getArray(ConfigConstants.TOPICS);
            int totalTopics = topics.size() * workersPerTopic;

            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));

            String topicList = getTopicFilter(topics);
            TopicFilter sourceTopicFilter = new Whitelist(topicList);
            List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter, totalTopics);

            threadPool = Executors.newFixedThreadPool(streams.size());

            logger.info("Starting to consume messages group: " + config.getString(ConfigConstants.GROUP_ID) +
                    " topic(s): " +  topicList);
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                threadPool.submit(() -> startConsumer(stream));
            }

            Runtime.getRuntime().addShutdownHook(new Thread() {
                // try to disconnect from ZK as gracefully as possible
                public void run() {
                    shutdown();
                }
            });

            startedResult.setResult(null);
        } catch (Exception ex) {
            logger.error(ex);
            startedResult.setFailure(ex);
        }

    }

    private Properties populateKafkaConfig(JsonObject config) {
        Properties consumerConfig = new Properties();
        consumerConfig.put("zookeeper.connect", String.format("%s:%s",
                config.getString(ConfigConstants.ZK_HOST, "localhost"),
                config.getString(ConfigConstants.ZK_PORT, "2181")));
        consumerConfig.put(ConfigConstants.BACKOFF_INCREMENT_MS,
                config.getString(ConfigConstants.BACKOFF_INCREMENT_MS, "100"));
        consumerConfig.put(ConfigConstants.BACKOFF_INCREMENT_MS,
                config.getString(ConfigConstants.AUTO_OFFSET_RESET, "smallest"));

        consumerConfig.put(ConfigConstants.GROUP_ID, getRequiredConfig(ConfigConstants.GROUP_ID));
        return consumerConfig;
    }

    private void startConsumer(KafkaStream<byte[],byte[]> stream) {
        try {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                String msg = new String(it.next().message());
                vertx.eventBus().publish(busAddress, msg);
            }
        } catch (Exception ex) {
            logger.error("Failed while consuming messages", ex);
            throw ex;
        }
    }

    private String getRequiredConfig(String key) {
        String value = container.config().getString(key, null);

        if (null == value) {
            throw new IllegalArgumentException(String.format("Required config value not found key: %s", key));
        }
        return value;
    }

    private String getTopicFilter(JsonArray topics) {
        StringBuilder builder = new StringBuilder();
        for (Object topic : topics) {
            logger.info("Preparing to filter on topic: " + topic);
            builder.append(topic.toString()).append("|");
        }
        if (builder.length() == 0) {
            throw new IllegalArgumentException("You must specify kafka topic(s) to filter on");
        }
        return builder.substring(0, builder.length() - 1);
    }

    private void shutdown() {
        if (threadPool != null) {
            threadPool.shutdown();
            threadPool = null;
        }

        if (consumer != null) {
            consumer.shutdown();
            consumer = null;
        }
    }
}