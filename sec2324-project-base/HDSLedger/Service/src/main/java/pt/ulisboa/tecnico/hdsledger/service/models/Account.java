package pt.ulisboa.tecnico.hdsledger.service.models;

import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class Account {
    private String id;
    private int amount;

    public Account(String id, int amount) {
        this.id = id;
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public int getAmount() {
        return amount;
    }
    public boolean checkTransaction(int value){
        if(amount-value < 0) return false;
        return true;
    }
}
