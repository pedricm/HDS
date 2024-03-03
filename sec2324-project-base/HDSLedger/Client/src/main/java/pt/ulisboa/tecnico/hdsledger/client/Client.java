package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;

public class Client {
    private ClientLibrary clientLibrary;
    public Client(Link linkToNodes, ProcessConfig config, ProcessConfig leaderConfig, ProcessConfig[] nodesConfig) {
        this.clientLibrary = new ClientLibrary(linkToNodes, config, leaderConfig, nodesConfig);
    }
    public void send(String command) {
        System.out.println("Sending command to the library: " + command);
        clientLibrary.send(command);
    }
}
