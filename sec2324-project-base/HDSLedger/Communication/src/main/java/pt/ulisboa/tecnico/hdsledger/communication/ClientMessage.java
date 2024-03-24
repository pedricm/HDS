package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import java.security.PublicKey;
import pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary;

public class ClientMessage {

    private String pubSource;
    private String pubDest = null;
    private int amount = -1;

    public ClientMessage(PublicKey pubSource, PublicKey pubDest, int amount) {
        this.pubSource = CryptoLibrary.pubKeyToString(pubSource);
        this.pubDest = CryptoLibrary.pubKeyToString(pubDest);
        this.amount = amount;
    }
    public ClientMessage(PublicKey pubSource) {
        this.pubSource = CryptoLibrary.pubKeyToString(pubSource);
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public PublicKey getPubSource() {
        if(this.pubSource == null) return null;
        return CryptoLibrary.stringToPubKey(this.pubSource);
    }

    public PublicKey getPubDest() {
        if(this.pubDest == null) return null;
        return CryptoLibrary.stringToPubKey(this.pubDest);
    }

    public int getAmount() { return amount; }

    public String toJson() {
        return new Gson().toJson(this);
    }
    public String toString(String senderId, int ident) {
        String tabs = "";
        for(int i =0; i<ident ;i++) tabs+="\t";

        String stringClass = getClass().getSimpleName() + "{\n"+tabs+"\t";
        if(pubDest == null) {
            stringClass += "CHECK_BALANCE IN ACCOUNT: " + this.pubSource + "\n"+tabs+"\tFROM SenderId: " + senderId;
        }
        else {
            stringClass += "TRANSACTION FROM ACCOUNT: " + this.pubSource + " (SenderId = " + senderId +")\n"+tabs+"\tTO ACCOUNT: " + this.pubDest +
                    "\n"+tabs+"\t AMOUNT: " + amount;
        }

        stringClass += "\n"+tabs+"}\n";
        return stringClass;
    }
}
