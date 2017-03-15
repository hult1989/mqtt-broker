package pku.netlab.hermes.broker.Impl;

import com.cyngn.kafka.consume.SimpleConsumer;
import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import pku.netlab.hermes.message.PendingMessage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hult on 3/9/17.
 */
public class Router extends AbstractVerticle{
    private final String DEFAULT_TOPIC = "EVENT";
    private Logger logger = LoggerFactory.getLogger(KafkaMQ.class);
    private KafkaPublisher producer;
    @Override
    public void start() throws Exception {
        JsonObject consumerConf = config().getJsonObject("consumer");
        consumerConf.put("topics", new JsonArray().add(DEFAULT_TOPIC));
        JsonObject producerConf = config().getJsonObject("producer");
        vertx.deployVerticle(MessageProducer.class.getName(), new DeploymentOptions().setConfig(producerConf), depPro-> {
            if (!depPro.succeeded()) {
                logger.error(depPro.cause().toString());
                System.exit(0);
            } else {
                this.logger.info("deploy router producer");
                this.producer = new KafkaPublisher(vertx.eventBus());
                vertx.deployVerticle(SimpleConsumer.class.getName(), new DeploymentOptions().setConfig(consumerConf), depCon-> {
                    if (!depCon.succeeded()) {
                        logger.error(depCon.cause().toString());
                        System.exit(0);
                    } else {
                        this.logger.info("deploy router consumer");
                        vertx.eventBus().consumer(SimpleConsumer.EVENTBUS_DEFAULT_ADDRESS, ((Message<JsonObject> msg) -> {
                            route(msg.body());
                        }));
                    }
                });
            }
        });
    }

    public void route(JsonObject message) {
        final int N_MATCH = 5;
        String payload = message.getString("value");
        List<String> target = new ArrayList<>(N_MATCH);
        for (int i = 0; i < N_MATCH; i += 1) {
            int randomID = (int)(1000000 * Math.random()) % 10000;
            target.add(String.format("%s%06d", "CC", randomID));
        }
        PendingMessage pending = new PendingMessage(payload, target);
        this.logger.info(pending);
        this.producer.send("broker-001", pending.toString());
    }

    public static void main(String[] args) throws IOException {
        String json = FileUtils.readFileToString(new File("demo.json"), "UTF-8");
        JsonObject config = new JsonObject(json).getJsonObject("kafka");
        Vertx.vertx().deployVerticle(Router.class.getName(), new DeploymentOptions().setConfig(config));
    }
}
