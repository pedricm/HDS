package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class ClientResponseMessage {

    // Value
    private boolean ack = false;
    private int amount =-1;

    public ClientResponseMessage(int amount, boolean ack) {
        this.ack = ack;
        this.amount = amount;
    }

    public boolean isAck() {
        return ack;
    }

    public int getAmount() {
        return amount;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
