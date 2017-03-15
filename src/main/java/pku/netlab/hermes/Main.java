package pku.netlab.hermes;

import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import pku.netlab.hermes.broker.CoreProcessor;

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
                processor.start();
            } catch(IOException e) {
                logger.fatal(e.getMessage(),e);
                System.exit(0);
            }
        }
    }



    public static void stop(String[] args) {
        System.exit(0);
    }

}
