package pt.ulisboa.tecnico.hdsledger.client;
//import pt.ulisboa.tecnico.hdsledger.communication.Link;
public class Client {
    ClientLibrary clientLibrary;
    public Client() {
        clientLibrary = new ClientLibrary();
    }
    public void send(String command) {
        System.out.println("Sending command to the library: " + command);
        clientLibrary.send(command);
    }
}
