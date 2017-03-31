package pku.netlab.hermes.router;

import hermes.dataobj.Event;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by hult on 3/9/17.
 */
public class Router extends AbstractVerticle {
    KafkaConsumer<String, byte[]> consumer;
    Map consumerConf;

    public Router(Map conf) {
        this.consumerConf = conf;
    }

    @Override
    public void start() throws Exception {
        this.consumer = KafkaConsumer.create(vertx, consumerConf, String.class, byte[].class);

        this.consumer.handler(record-> {
            Event e = Event.fromBytes(record.value());
            System.out.println(e);
        });
        this.consumer.subscribe("broker_219.223.196.132");
    }


    public static void main(String[] args) throws IOException {
        JsonObject json = new JsonObject(FileUtils.readFileToString(new File("router.json"), "UTF-8"));
        Map<String, String> conf = json.stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
        Router router = new Router(conf);
        Vertx.vertx().deployVerticle(router);

    }
}
