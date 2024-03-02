package pt.ulisboa.tecnico.hdsledger.service.models;

import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class ClientRepository {
    private final Map<String, HashSet<Integer>> doneList = new ConcurrentHashMap<>();

    public ClientRepository() {
    }
    public void messageDone(String client, int messageId) {
        doneList.putIfAbsent(client, new HashSet<Integer>());
        doneList.get(client).add(messageId);
    }
    public boolean checkIfDone(String client, int messageId) {
        doneList.putIfAbsent(client, new HashSet<Integer>());
        return doneList.get(client).contains(messageId);
    }
}
