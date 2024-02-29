package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;



public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link linkToNodes;
    private ProcessConfig nodeConfig;
    private ProcessConfig leaderConfig;
    private ProcessConfig[] nodeConfigs;

    public ClientLibrary(Link link, ProcessConfig config, ProcessConfig leaderConf, ProcessConfig[] nodesConfig) {
        this.linkToNodes = link;
        this.nodeConfig = config;
        this.leaderConfig = leaderConf;
        this.nodeConfigs = nodesConfig;
    }

    public void send(String command) {
        System.out.println("Sending command: " + command);
        //PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        /*ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.APPEND)
                .setConsensusInstance(consensusInstance)
                .setRound(round)
                .setMessage(prepareMessage.toJson())
                .setReplyTo(senderId)
                .setReplyToMessageId(senderMessageId)
                .build();

        this.link.broadcast(consensusMessage);*/
    }
}
