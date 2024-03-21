package pt.ulisboa.tecnico.hdsledger.client;
import java.util.Scanner;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.service.services.NodeService;
import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfigBuilder;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Level;

public class Main {
    private static final CustomLogger LOGGER = new CustomLogger(Main.class.getName());
    private static String nodesConfigPath = "../Service/src/main/resources/";

    private static String keysPath = "../Service/src/main/resources/keys/";

    public static void main(String[] args) {
        try {
            // Command line arguments
            String id = args[0];
            nodesConfigPath += args[1];

            // Create configuration instances
            ProcessConfig[] configs = new ProcessConfigBuilder().fromFile(nodesConfigPath);
            ProcessConfig[] nodeConfigs = Arrays.stream(configs)
                    .filter(config -> !config.isClient())
                    .toArray(ProcessConfig[]::new);
            ProcessConfig[] clientConfigs = Arrays.stream(configs)
                    .filter(ProcessConfig::isClient)
                    .toArray(ProcessConfig[]::new);
            ProcessConfig ClientConfig = Arrays.stream(clientConfigs).filter(c -> c.getId().equals(id)).findAny().get();

            LOGGER.log(Level.INFO, MessageFormat.format("{0} - Running at {1}:{2}; tests: {3}",
                    ClientConfig.getId(), ClientConfig.getHostname(), ClientConfig.getPort(), Arrays.toString(ClientConfig.getTests())));

            // Abstraction to send and receive messages
            Link linkToNodes = new Link(ClientConfig, ClientConfig.getPort(), nodeConfigs, clientConfigs,
                    ConsensusMessage.class);

            Client client = new Client(linkToNodes, ClientConfig, nodeConfigs, keysPath);
            Scanner parser = new Scanner(System.in);

            printUsage();

            while (true) {
                System.out.println("Type \"help\", for more information");
                System.out.print("> ");
                String command = parser.nextLine();  // Read user input

                switch(command) {
                    case "help":
                        printUsage();
                        break;
                    case "transfer":
                        //client.transfer();
                        break;
                    case "check_balance":
                        //client.check_balance();
                        break;
                    default:
                        System.out.println("Invalid command");
                        break;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printUsage(){
        System.out.println("\n-----------------------------------------------");
        System.out.print("\tAvailable commands:\n" +
                "transfer: transfer money between 2 accounts\n" +
                "check_balance: obtain the balance a given account\n\n");
    }
}