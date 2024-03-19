package pt.ulisboa.tecnico.hdsledger.client;
import java.security.PublicKey;
import java.util.Scanner;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ConsensusMessageBuilder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;

import static pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary.readPublicKey;


public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link link;
    private ProcessConfig nodeConfig;
    private ProcessConfig[] nodeConfigs;
    // num msg -> sender ID -> message
    private final Map<Integer, Map<String, ConsensusMessage>> bucket = new ConcurrentHashMap<>();
    private int quorumSize;
    private int msgCounter = 0;

    private String keysPath;

    private static String nodeKeysPath = "../Service/src/main/resources/keys/";

    public ClientLibrary(Link linkToNodes, ProcessConfig config, ProcessConfig[] nodesConfig, String keysPath) {
        this.link = linkToNodes;
        this.nodeConfig = config;
        this.nodeConfigs = nodesConfig;
        int nodeCount = this.nodeConfigs.length;
        int f = Math.floorDiv(nodeCount - 1, 3);
        quorumSize = Math.floorDiv(nodeCount + f, 2) + 1;
        this.keysPath = keysPath;

        listen();
    }

    public void transfer(String src_account, String dest_account, int amount) {
        System.out.println("Sending transfer request { src: " +
                            src_account + " ;dst: " + dest_account +
                            " ;amount: " + amount + " }");

        PublicKey src_pub_key = readPublicKey(keysPath + "key_" + src_account + "_pub.key");
        PublicKey dest_pub_key = readPublicKey(keysPath + "key_" + dest_account + "_pub.key");
        // Build transfer Message
    }

    public void check_balance(String account){
        System.out.println("Sending transfer request { account: " + account + " }");

        PublicKey src_pub_key = readPublicKey(keysPath + "key_" + account + "_pub.key");
        // Build transfer Message
    }

    public void send(String msg) {
        System.out.println("Sending command: " + msg);

        //PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(nodeConfig.getId(), Message.Type.APPEND)
                .setMessage(msg)
                .setReplyToMessageId(msgCounter++)
                .build();

        this.link.broadcast(consensusMessage);

        int responses = 0;
        while (responses < this.quorumSize) {
            if (bucket.containsKey(msgCounter - 1))
                responses = bucket.get(msgCounter - 1).size();
        }
    }
    public void addMessageToBucket(ConsensusMessage message) {
        int messageId = message.getReplyToMessageId() / nodeConfigs.length;
        String senderId = message.getSenderId();
        System.out.println("Adding message to bucket with id: " + messageId+ " and sender: " + senderId);
        bucket.putIfAbsent(messageId, new ConcurrentHashMap<>());
        bucket.get(messageId).put(senderId, message);

    }
    public void listen() {
        try {
            // Thread to listen on every request
            new Thread(() -> {
                try {
                    while (true) {
                        Message message = this.link.receive();

                        // Separate thread to handle each message
                        new Thread(() -> {
                            if (message.getType().equals(Message.Type.ACK_CLIENT)) {
                                System.out.println("Received response with id: " + ((ConsensusMessage) message).getReplyToMessageId());
                                addMessageToBucket((ConsensusMessage) message);
                            }

                        }).start();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
