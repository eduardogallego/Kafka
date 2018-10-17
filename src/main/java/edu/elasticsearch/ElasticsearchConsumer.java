package edu.elasticsearch;

import com.google.gson.JsonParser;
import edu.config.ConfigSt;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticsearchConsumer {

    private static final ConfigSt CONFIG = ConfigSt.getInstance();
    private static final int ES_PORT = Integer.parseInt(CONFIG.getConfig("elasticsearch_port"));
    private static final JsonParser JSON_PARSER = new JsonParser();
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
    private static final String ES_HOSTNAME = CONFIG.getConfig("elasticsearch_hostname"),
            ES_INDEX = CONFIG.getConfig("elasticsearch_index"),
            ES_SCHEME = CONFIG.getConfig("elasticsearch_scheme"),
            ES_TYPE = CONFIG.getConfig("elasticsearch_type"),
            KAFKA_CONSUMER_GROUP = CONFIG.getConfig("kafka_consumer_group"),
            KAFKA_SERVER = CONFIG.getConfig("kafka_server"),
            KAFKA_TOPIC = CONFIG.getConfig("kafka_topic");

    public static void main(String[] args) {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(ES_HOSTNAME, ES_PORT, ES_SCHEME)))) {
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto-commit offsets
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList(KAFKA_TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
                int recordsCount = records.count();
                LOGGER.info("Received " + recordsCount + " records");
                if (recordsCount > 0) {
                    BulkRequest bulkRequest = new BulkRequest();
                    for (ConsumerRecord<String, String> record : records) {
                        // Set ID to make it idempotent (exactly one)
                        // 1. Kafka generic ID
                        // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                        // 2. Twitter feed specific ID
                        String tweetJson = record.value();
                        try {
                            String id = JSON_PARSER.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
                            IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_TYPE, id)
                                    .source(tweetJson, XContentType.JSON);
                            bulkRequest.add(indexRequest);
                        } catch (Exception ex) {
                            LOGGER.warn("Skipping bad data: " + tweetJson);
                        }
                    }
                    BulkResponse bulkResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    LOGGER.info("Commiting offsets...");
                    consumer.commitSync();
                    LOGGER.info("Offsets have been commited");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        LOGGER.error("InterruptedException: " + ex.getMessage(), ex);
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Exception: " + ex.getMessage(), ex);
        }
    }
}
