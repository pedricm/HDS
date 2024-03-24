package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.BlockMessage;

public class RoundChangeMessage {

    // Value
    private String value = null;
    private int round =-1;

    public RoundChangeMessage(BlockMessage value, int round) {
        this.value = value.toJson();
        this.round = round;
    }
    public RoundChangeMessage(String value, int round) {
        this.value = value;
        this.round = round;
    }

    public int getPreparedRound() {
        return round;
    }

    public String getPreparedValue() {
        return value;
    }

    public BlockMessage deserializePreparedValue() {
        return new Gson().fromJson(this.value, BlockMessage.class);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
