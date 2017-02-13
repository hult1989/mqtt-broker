package pku.netlab.hermes;

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
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import pku.netlab.hermes.broker.CoreProcessor;
import pku.netlab.hermes.broker.MQTTBroker;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


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
        if(confFilePath!=null) {
            try {
                String json = FileUtils.readFileToString(new File(confFilePath), "UTF-8");
                JsonObject config = new JsonObject(json);

                CoreProcessor processor = new CoreProcessor(config);
                processor.init();

                deployClusterBroker(config);
            } catch(IOException e) {
                logger.fatal(e.getMessage(),e);
                System.exit(0);
            }
        }
    }

    private static void deployClusterBroker(JsonObject config) {
        JsonObject zkConfig = config.getJsonObject("zookeepers");
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkConfig.getString("zookeeperHosts"))
                .namespace(zkConfig.getString("rootPath", "io.vertx"))
                .sessionTimeoutMs(zkConfig.getInteger("sessionTimeout", 20000))
                .connectionTimeoutMs(zkConfig.getInteger("connectTimeout", 3000))
                .retryPolicy(new ExponentialBackoffRetry(
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("initialSleepTime", 1000),
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("maxTimes", 5),
                        zkConfig.getJsonObject("retry", new JsonObject()).getInteger("intervalTimes", 10000))
                ).build();
        curator.start();

        String brokerID = config.getJsonObject("broker").getString("broker_id");
        //ZookeeperClusterManager manager = new ZookeeperClusterManager(zkConfig);
        ZookeeperClusterManager manager = new ZookeeperClusterManager(curator, brokerID);
        VertxOptions options = new VertxOptions().setClusterManager(manager);


        Vertx.clusteredVertx(options, res-> {
            if (res.succeeded()) {
                Vertx vertx = res.result();

                MQTTBroker.clusterID = manager.getNodeID();
                MQTTBroker.brokerID = config.getJsonObject("broker").getString("broker_id");
                vertx.deployVerticle(MQTTBroker.class.getName(),
                        new DeploymentOptions().setConfig(config).setInstances(Runtime.getRuntime().availableProcessors()));
                logger.info("cluster broker deployed with id: " + MQTTBroker.clusterID);

            } else {
                logger.error("fail to cluster brokers: " + res.cause().getMessage());
                System.exit(0);
            }
        });
    }

    public static void stop(String[] args) {
        System.exit(0);
    }

}
