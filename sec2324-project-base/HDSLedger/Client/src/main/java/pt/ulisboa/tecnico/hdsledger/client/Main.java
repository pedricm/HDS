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

    public static void main(String[] args) {
            try {
                // Command line arguments
                String id = args[0];
                nodesConfigPath += args[1];

                // Create configuration instances
                ProcessConfig[] nodeConfigs = new ProcessConfigBuilder().fromFile(nodesConfigPath);
                ProcessConfig leaderConfig = Arrays.stream(nodeConfigs).filter(ProcessConfig::isLeader).findAny().get();
                ProcessConfig nodeConfig = Arrays.stream(nodeConfigs).filter(c -> c.getId().equals(id)).findAny().get();

                LOGGER.log(Level.INFO, MessageFormat.format("{0} - Running at {1}:{2}; is leader: {3}",
                        nodeConfig.getId(), nodeConfig.getHostname(), nodeConfig.getPort(),
                        nodeConfig.isLeader()));

                // Abstraction to send and receive messages
                Link linkToNodes = new Link(nodeConfig, nodeConfig.getPort(), nodeConfigs,
                        ConsensusMessage.class);

                Client client = new Client(linkToNodes, nodeConfig, leaderConfig, nodeConfigs); //ipServer, portServer, privateClientKeyPath, publicClientKeyPath, privateClient2KeyPath, publicClient2KeyPath, publicServerKeyPath
                Scanner parser = new Scanner(System.in);

                printUsage();

                while (true) {
                    System.out.print("> ");
                    String command = parser.nextLine();  // Read user input

                    if (command.equals("/q")) {
                        System.out.println("Quiting!");
                        return;
                    }
                    client.send(command);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static void printUsage(){
            System.out.println("\n-----------------------------------------------");
            System.out.println("Insert strings!");
            System.out.println("/q to quit!\n\n");
        }
}