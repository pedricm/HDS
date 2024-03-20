package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class CommitMessage {

    // Value
    private String value;

    public CommitMessage(ConsensusMessage value) {
        this.value = value.toJson();
    }
    public CommitMessage(String value) {
        this.value = value;
    }
    public String getValue() {
        return value;
    }
    public ConsensusMessage deserializeValue() {
        return new Gson().fromJson(this.value, ConsensusMessage.class);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
