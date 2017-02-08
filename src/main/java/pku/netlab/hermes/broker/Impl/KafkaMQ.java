package pku.netlab.hermes.broker.Impl;

import com.cyngn.kafka.consume.SimpleConsumer;
import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import pku.netlab.hermes.broker.IMessageQueue;

/**
 * Created by hult on 2/8/17.
 */
public class KafkaMQ implements IMessageQueue{
    private Handler<PublishMessage> handler;
    private KafkaPublisher publisher;


    public KafkaMQ(Vertx vertx, JsonObject config) {
        String brokerID = config.getJsonObject("broker").getString("broker_id");
        JsonObject consumerConf = config.getJsonObject("kafka").getJsonObject("consumer");

        //eb address: broker-001/kafka.message.consumer, broker-001/kafka.message.producer
        String consumerAddress = String.join("/", SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, brokerID);
        consumerConf.put("eventbus.address", consumerAddress);
        consumerConf.put("topics", new JsonArray().add(brokerID));
        deployConsumer(vertx, consumerConf, consumerAddress);

        JsonObject producerConf = config.getJsonObject("kafka").getJsonObject("producer");
        String producerAddress = String.join("/", MessageProducer.EVENTBUS_DEFAULT_ADDRESS, brokerID);
        producerConf.put("eventbus.address", producerAddress);
        deployProducer(vertx, producerConf, producerAddress);
     }

    @Override
    public void enqueue(PublishMessage message) {
        publisher.send(message.toString());
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

    private void deployConsumer(Vertx vertx, JsonObject consumerConf, String consumerAddress) {
        vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(consumerConf), deploy-> {
            if (!deploy.succeeded()) {
                System.err.println("Failed to deploy kafka consumer for: " + deploy.cause().getMessage());
                System.exit(0);
            } else {
                vertx.eventBus().consumer(consumerAddress, msg-> {
                    try {
                        handler.handle((PublishMessage) msg.body());
                    } catch (Exception e) {
                        System.out.println(msg.body().toString());
                    }
                });
            }
        });
    }

    @Override
    public void setMessageHandler(Handler<PublishMessage> handler) {
        this.handler = handler;
    }
}
