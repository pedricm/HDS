package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.utilities.ObjToString;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import java.io.Serializable;

public class TransactionMessage implements Serializable {

    // Value
    private String client;

    private int gas;

    public TransactionMessage(String client, int gas) {
        this.client = client;
        this.gas = gas;
    }

    public TransactionMessage(ConsensusMessage client, int gas) {
        this.client = client.toJson();
        this.gas = gas;
    }

    public String getClient() {
        return client;
    }

    public void setClient(ConsensusMessage client) {
        this.client = client.toJson();
    }

    public int getGas() {
        return gas;
    }

    public ConsensusMessage deserializeConsensusMessage() {
        return new Gson().fromJson(this.client, ConsensusMessage.class);
    }
    public String toJson() {
        return new Gson().toJson(this);
    }
    public String toString(int ident) {
        String tabs = "";
        for(int i =0; i<ident ;i++) tabs+="\t";

        String stringClass = getClass().getSimpleName() + "{\n"+tabs+"\tGasPrice= " + this.gas +"\n"+tabs+"\t";

        stringClass += this.deserializeConsensusMessage().toString(ident+1) +tabs+"}\n";
        return stringClass;
    }
}
