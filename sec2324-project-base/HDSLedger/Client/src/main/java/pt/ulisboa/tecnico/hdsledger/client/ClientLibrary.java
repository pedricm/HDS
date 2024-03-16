package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ConsensusMessageBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link link;
    private ProcessConfig nodeConfig;
    private ProcessConfig[] nodeConfigs;
    private int quorumSize;
    private int msgCounter = 0;

    private static String nodeKeysPath = "../Service/src/main/resources/keys/";

    public ClientLibrary(Link linkToNodes, ProcessConfig config, ProcessConfig[] nodesConfig) {
        this.link = linkToNodes;
        this.nodeConfig = config;
        this.nodeConfigs = nodesConfig;
        int nodeCount = this.nodeConfigs.length;
        int f = Math.floorDiv(nodeCount - 1, 3);
        quorumSize = Math.floorDiv(nodeCount + f, 2) + 1;
    }

    public void send(String msg) {
        System.out.println("Sending command: " + msg);

        //PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(nodeConfig.getId(), Message.Type.APPEND)
                .setMessage(msg)
                .setReplyToMessageId(msgCounter++)
                .build();

        this.link.broadcast(consensusMessage);

        Message message = null;
        Set<String> iDs = new HashSet<>();
        int responses = 0;
        while (responses < this.quorumSize) {
            message = receive();
            if (message != null && message.getType() == Message.Type.ACK_CLIENT && iDs.add(message.getSenderId())) {
                responses++;
                System.out.println("Received response: " + message + " id: " + message.getMessageId());
            }
        }
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
