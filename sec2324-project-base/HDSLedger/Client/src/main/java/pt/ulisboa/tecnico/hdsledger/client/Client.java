package pt.ulisboa.tecnico.hdsledger.client;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;

import java.util.Scanner;

public class Client {
    private ClientLibrary clientLibrary;
    public Client(Link linkToNodes, ProcessConfig config, ProcessConfig[] nodesConfig, String keysPath) {
        this.clientLibrary = new ClientLibrary(linkToNodes, config, nodesConfig, keysPath);
    }
    public void send(String command) {
        System.out.println("Sending command to the library: " + command);
        clientLibrary.send(command);
    }

    public void transfer() {
        Scanner parser = new Scanner(System.in);

        System.out.print("Source Account: ");
        String src_account = parser.nextLine();

        System.out.print("\nDestination Account: ");
        String dest_account = parser.nextLine();

        boolean valid_value = false;
        int amount = 0;
        while (!valid_value) {
            try {
                System.out.print("\nAmount: ");
                amount = Integer.parseInt(parser.nextLine());
                valid_value = true;
            } catch (NumberFormatException e) {
                valid_value = false;
            }
        }

        clientLibrary.transfer(src_account, dest_account, amount);
    }

    public void check_balance() {
        Scanner parser = new Scanner(System.in);

        System.out.print("Account: ");
        String account = parser.nextLine();

        clientLibrary.check_balance(account);
    }
}
