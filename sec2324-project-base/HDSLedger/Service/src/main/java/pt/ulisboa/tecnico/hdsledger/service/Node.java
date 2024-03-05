package pt.ulisboa.tecnico.hdsledger.service;

import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.service.services.NodeService;
import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfigBuilder;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.logging.Level;

public class Node {

    private static final CustomLogger LOGGER = new CustomLogger(Node.class.getName());
    // Hardcoded path to files
    private static String nodesConfigPath = "src/main/resources/";
    private static String keysPath = "src/main/resources/keys/";

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
            ProcessConfig nodeConfig = Arrays.stream(nodeConfigs).filter(c -> c.getId().equals(id)).findAny().get();

            LOGGER.log(Level.INFO, MessageFormat.format("{0} - Running at {1}:{2}; is client; {3}",
                    nodeConfig.getId(), nodeConfig.getHostname(), nodeConfig.getPort(), nodeConfig.isClient()));

            // Abstraction to send and receive messages
            Link linkToNodes = new Link(nodeConfig, nodeConfig.getPort(), nodeConfigs, clientConfigs,
                    ConsensusMessage.class);

            // Services that implement listen from UDPService
            NodeService nodeService = new NodeService(linkToNodes, nodeConfig,
                    nodeConfigs, keysPath);

            nodeService.listen();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
