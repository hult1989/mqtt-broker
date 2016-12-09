package io.github.giovibal.mqtt;

import com.hazelcast.config.*;
import io.github.giovibal.mqtt.pushservice.DBClient;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Giovanni Baleani on 13/11/2015.
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        start(args);
    }

    static CommandLine cli(String[] args) {
        CLI cli = CLI.create("java -jar <mqtt-broker>-fat.jar")
                .setSummary("A vert.x MQTT Broker")
                .addOption(new Option()
                                .setLongName("conf")
                                .setShortName("c")
                                .setDescription("broker config file (in json format)")
                                .setRequired(true)
                );

        // parsing
        CommandLine commandLine = null;
        try {
            List<String> userCommandLineArguments = Arrays.asList(args);
            commandLine = cli.parse(userCommandLineArguments);
        } catch(CLIException e) {
            // usage
            StringBuilder builder = new StringBuilder();
            cli.usage(builder);
            System.out.println(builder.toString());
//            throw e;
        }
        return commandLine;
    }

    public static void start(String[] args) {
        CommandLine commandLine = cli(args);
        if(commandLine == null)
            System.exit(-1);

        String confFilePath = commandLine.getOptionValue("c");

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        if(confFilePath!=null) {
            try {
                String json = FileUtils.readFileToString(new File(confFilePath), "UTF-8");
                JsonObject config = new JsonObject(json);
                deploymentOptions.setConfig(config);
            } catch(IOException e) {
                logger.fatal(e.getMessage(),e);
            }
        }


        VertxOptions options = new VertxOptions();
        Vertx vertx = Vertx.vertx(options);

        //初始化DB
        DBClient.init(vertx);

        vertx.deployVerticle(MQTTBroker.class.getName(), deploymentOptions);
        vertx.deployVerticle("io.github.giovibal.mqtt.pushservice.PushServer");
            //vertx.deployVerticle("io.github.giovibal.mqtt.pushservice.PushVerticle");

    }
    public static void stop(String[] args) {
        System.exit(0);
    }

}
