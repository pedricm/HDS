package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.ClientMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ClientMessageBuilder;


public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link link;
    private ProcessConfig nodeConfig;
    private ProcessConfig leaderConfig;
    private ProcessConfig[] nodeConfigs;

    public ClientLibrary(Link linkToNodes, ProcessConfig config, ProcessConfig leaderConf, ProcessConfig[] nodesConfig) {
        this.link = linkToNodes;
        this.nodeConfig = config;
        this.leaderConfig = leaderConf;
        this.nodeConfigs = nodesConfig;
    }

    public void send(String msg) {
        System.out.println("Sending command: " + msg);
        //PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        ClientMessage clientMessage = new ClientMessageBuilder(nodeConfig.getId(), Message.Type.APPEND).setMessage(msg).setReplyTo(nodeConfig.getId()).setReplyToMessageId(0).build();

        this.link.broadcast(clientMessage);
    }
}
