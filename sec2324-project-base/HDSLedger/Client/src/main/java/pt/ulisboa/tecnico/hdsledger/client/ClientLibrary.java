package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
//import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;

public class ClientLibrary {
    //private static final CustomLogger LOGGER = new CustomLogger(ClientLibrary.class.getName());
    //Link
    private Link linkToNodes;

    public ClientLibrary(Link link) {
        this.linkToNodes = link;

    }

    public void send(String command) {
        System.out.println("Sending command: " + command);
    }
}
