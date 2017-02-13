package pku.netlab.hermes.broker.Impl;

import com.cyngn.kafka.consume.SimpleConsumer;
import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.IMessageQueue;

/**
 * Created by hult on 2/8/17.
 */
public class KafkaMQ implements IMessageQueue{
    private Handler<JsonObject> consumer;
    private KafkaPublisher publisher;
    private Logger logger = LoggerFactory.getLogger(KafkaMQ.class);

    public KafkaMQ(Vertx vertx, JsonObject config, Handler<JsonObject> handler) {
        String brokerID = config.getJsonObject("broker").getString("broker_id");
        JsonObject consumerConf = config.getJsonObject("kafka").getJsonObject("consumer");

        //eb address: broker-001/kafka.message.consumer, broker-001/kafka.message.producer
        String consumerAddress = String.join("/", SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, brokerID);
        consumerConf.put("eventbus.address", consumerAddress);
        consumerConf.put("topics", new JsonArray().add(brokerID));
        deployConsumer(vertx, consumerConf, consumerAddress, handler);

        JsonObject producerConf = config.getJsonObject("kafka").getJsonObject("producer");
        String producerAddress = String.join("/", MessageProducer.EVENTBUS_DEFAULT_ADDRESS, brokerID);
        producerConf.put("eventbus.address", producerAddress);
        deployProducer(vertx, producerConf, producerAddress);
        logger.info("kafka deployed at " + Thread.currentThread().getName());
     }

    @Override
    public void enQueue(PublishMessage message) {
        publisher.send("EVENT", message.toString());
    }

    private void deployProducer(Vertx vertx, JsonObject producerConf, String producerAddress) {
        vertx.deployVerticle(MessageProducer.class.getName(), new DeploymentOptions().setConfig(producerConf), deploy-> {
            if (!deploy.succeeded()) {
                System.err.println("Failed to deploy kafka producer for: " + deploy.cause().getMessage());
                System.exit(0);
            } else {
                this.publisher = new KafkaPublisher(vertx.eventBus(), producerAddress);
                vertx.setPeriodic(2_000, id-> {
                    publisher.send("PUBLISH", "" + System.currentTimeMillis());
                });
            }
        });
    }

    private void deployConsumer(Vertx vertx, JsonObject consumerConf, String consumerAddress, Handler<JsonObject> msgHandler) {
        this.consumer = msgHandler;
        vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(consumerConf), deploy-> {
            if (!deploy.succeeded()) {
                System.err.println("Failed to deploy kafka consumer for: " + deploy.cause().getMessage());
                System.exit(0);
            } else {
                vertx.eventBus().consumer(consumerAddress, msg-> {
                    consumer.handle((JsonObject)msg.body());
                });
            }
        });
    }
}
