package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import java.security.PublicKey;

public class ClientMessage {

    private PublicKey pubSource;
    private PublicKey pubDest = null;
    private int amount = -1;

    public ClientMessage(PublicKey pubSource, PublicKey pubDest, int amount) {
        this.pubSource = pubSource;
        this.pubDest = pubDest;
        this.amount = amount;
    }
    public ClientMessage(PublicKey pubSource) {
        this.pubSource = pubSource;
    }

    public PublicKey getPubSource() { return pubSource; }

    public PublicKey getPubDest() { return pubDest; }

    public int getAmount() { return amount; }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
