package pt.ulisboa.tecnico.hdsledger.client;
import java.util.Scanner;
import java.io.IOException;
import pt.ulisboa.tecnico.hdsledger.client.Client;

public class Main {

        public static void main(String[] args) {
            if (args.length != 1) {
                System.out.println(/*"Usage: java -jar Client.jar "*/"<Args Path>");
                return;
            }

            Client client = new Client(); //ipServer, portServer, privateClientKeyPath, publicClientKeyPath, privateClient2KeyPath, publicClient2KeyPath, publicServerKeyPath
            Scanner parser = new Scanner(System.in);

            printUsage();

            while (true) {
                System.out.print("> ");
                String command = parser.nextLine();  // Read user input

                if (command.equals("/q")) {
                    System.out.println("Quiting!");
                    return;
                }
                try {
                    client.send(command);
                } catch (IOException e) {
                    e.printStackTrace();
            }
        }

        private static void printUsage(){
            System.out.println("\n-----------------------------------------------");
            System.out.println("Insert strings!");
            System.out.println("/q to quit!");
        }
}