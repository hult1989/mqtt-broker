package pku.netlab.hermes.broker.Impl;

import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.IMessageQueue;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hult on 2/8/17.
 */
public class KafkaMQ implements IMessageQueue{
    private Handler<byte[]> consumer;
    private KafkaPublisher publisher;
    private final String DEFAULT_TOPIC;
    private Logger logger = LoggerFactory.getLogger(KafkaMQ.class);

    public KafkaMQ(Vertx vertx, JsonObject kafkaConfig, Handler<byte[]> handler) {
        String brokerID = kafkaConfig.getString("brokerID");
        JsonObject consumerConf = kafkaConfig.getJsonObject("consumer");

        consumerConf.put("topics", new JsonArray().add(brokerID));
        deployNewConsumer(vertx, brokerID, handler);

        JsonObject producerConf = kafkaConfig.getJsonObject("producer");
        this.DEFAULT_TOPIC = producerConf.getString("default_topic");
        deployProducer(vertx, producerConf);
        logger.info(String.format("kafka deployed at %s with TOPIC %s",
                Thread.currentThread().getName(), consumerConf.getJsonArray("topics")));
     }

    @Override
    public void enQueue(PublishMessage message) {
        publisher.send(DEFAULT_TOPIC, message.getPayloadAsString());
    }

    private void deployProducer(Vertx vertx, JsonObject producerConf) {
        vertx.deployVerticle(MessageProducer.class.getName(), new DeploymentOptions().setConfig(producerConf), deploy-> {
            if (!deploy.succeeded()) {
                System.err.println("Failed to deploy kafka producer for: " + deploy.cause().getMessage());
                System.exit(0);
            } else {
                this.publisher = new KafkaPublisher(vertx.eventBus());
            }
        });
    }

    private void deployNewConsumer(Vertx vertx, String brokerID, Handler<byte[]> byteHandler) {
        this.consumer = byteHandler;
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "server:9092");
        config.put("acks", "1");
        config.put("group.id", "hermesMatcher");
        config.put("auto.offset.reset", "latest");
        config.put("enable.auto.commit", "false");
        KafkaConsumer<String, byte[]> consumer = KafkaConsumer.create(vertx, config, String.class, byte[].class);
        consumer.handler(msg-> {
            byteHandler.handle(msg.value());
        });
        consumer.subscribe(brokerID);

    }

    /*
    private void deployConsumer(Vertx vertx, JsonObject consumerConf, Handler<JsonObject> msgHandler) {
        this.consumer = msgHandler;
        vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(consumerConf), deploy-> {
            if (!deploy.succeeded()) {
                System.err.println("Failed to deploy kafka consumer for: " + deploy.cause().getMessage());
                System.exit(0);
            } else {
                vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, (Message<JsonObject> msg)-> {
                    logger.info(msg.body());
                    consumer.handle(msg.body());
                });
            }
        });
    }
    */

    protected void publish(String topic, String pendingMessage) {
        this.publisher.send(topic, pendingMessage);
    }

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
    }
}
