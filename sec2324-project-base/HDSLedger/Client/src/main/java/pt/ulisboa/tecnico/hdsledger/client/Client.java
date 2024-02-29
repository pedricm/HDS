package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
public class Client {
    Link linkToNodes = new Link(nodeConfig, nodeConfig.getPort(), nodeConfigs, ConsensusMessage.class);
    public Client() {}
    public void send(String command) {
        System.out.println("Sending command: " + command);
    }
}
