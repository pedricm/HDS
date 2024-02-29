package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
public class Client {
    private ClientLibrary clientLibrary;
    public Client(Link linkToNodes) {
        this.clientLibrary = new ClientLibrary(linkToNodes);
    }
    public void send(String command) {
        System.out.println("Sending command to the library: " + command);
        clientLibrary.send(command);
    }
}
