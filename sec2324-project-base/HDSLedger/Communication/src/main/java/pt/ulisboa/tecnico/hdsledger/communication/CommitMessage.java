package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.BlockMessage;

public class CommitMessage {

    // Value
    private String value;

    public CommitMessage(BlockMessage value) {
        this.value = value.toJson();
    }
    public CommitMessage(String value) {
        this.value = value;
    }
    public String getValue() {
        return value;
    }
    public BlockMessage deserializeValue() {
        return new Gson().fromJson(this.value, BlockMessage.class);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
