package pku.netlab.hermes.broker.Impl;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.IMessageQueue;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hult on 2/8/17.
 */
public class KafkaMQ implements IMessageQueue{
    private KafkaConsumer<String, byte[]> consumer;
    private KafkaProducer<String, byte[]> producer;
    private final String DEFAULT_TOPIC;
    private Logger logger = LoggerFactory.getLogger(KafkaMQ.class);

    public KafkaMQ(Vertx vertx, JsonObject kafkaConfig, Handler<byte[]> handler) {
        String brokerID = kafkaConfig.getString("brokerID");
        JsonObject consumerConf = kafkaConfig.getJsonObject("consumer");

        consumerConf.put("topic", brokerID);
        deployConsumer(vertx, consumerConf, handler);

        JsonObject producerConf = kafkaConfig.getJsonObject("producer");
        this.DEFAULT_TOPIC = producerConf.getString("default_topic");
        deployProducer(vertx, producerConf);
     }

    @Override
    public void enQueue(PublishMessage message) {
        KafkaProducerRecord<String, byte[]> record = KafkaProducerRecord.create(this.DEFAULT_TOPIC, message.getPayload().array());
        this.producer.write(record, ar-> {
            if (ar.failed()) {
                logger.error("failed to write to kafka for " + ar.cause().toString());
            }
        });
    }

    private void deployProducer(Vertx vertx, JsonObject producerConf) {
        Map<String, String> confMap = producerConf.stream().collect(Collectors.toMap(Map.Entry::getKey, e-> (String) e.getValue()));
        this.producer = KafkaProducer.create(vertx, confMap, String.class, byte[].class);
    }

    private void deployConsumer(Vertx vertx, JsonObject consumerConf, Handler<byte[]> msgHandler) {
        Map<String, String> confMap = consumerConf.stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
        this.consumer = KafkaConsumer.create(vertx, confMap, String.class, byte[].class);
        this.consumer.handler(msg -> msgHandler.handle(msg.value()));
        this.consumer.subscribe(confMap.get("topic"), sub-> {
            if (sub.succeeded()) {
                logger.info(String.format("kafka deployed at %s with TOPIC %s",
                        Thread.currentThread().getName(), confMap.get("topic")));
            } else {
                System.out.println("failed to deploy kafka consumer for " + sub.cause().toString());
                System.exit(0);
            }
        });
    }
}
