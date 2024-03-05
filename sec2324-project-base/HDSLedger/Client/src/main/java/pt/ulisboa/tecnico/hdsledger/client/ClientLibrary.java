package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ConsensusMessageBuilder;

import java.io.IOException;


public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link link;
    private ProcessConfig nodeConfig;
    private ProcessConfig leaderConfig;
    private ProcessConfig[] nodeConfigs;

    private static String nodeKeysPath = "../Service/src/main/resources/keys/";

    public ClientLibrary(Link linkToNodes, ProcessConfig config, ProcessConfig leaderConf, ProcessConfig[] nodesConfig) {
        this.link = linkToNodes;
        this.nodeConfig = config;
        this.leaderConfig = leaderConf;
        this.nodeConfigs = nodesConfig;
    }

    public void send(String msg) {
        System.out.println("Sending command: " + msg);

        //PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(nodeConfig.getId(), Message.Type.APPEND)
                .setMessage(msg)
                .build();

        this.link.broadcast(consensusMessage);

        new Thread(() -> {
            Message message = null;
            while (message == null) {
                message = receive();
            }
        }).start();
    }

    public Message receive() {
        try {
            return this.link.receive();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
