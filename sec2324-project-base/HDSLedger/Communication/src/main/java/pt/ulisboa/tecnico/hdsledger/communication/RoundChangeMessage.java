package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class RoundChangeMessage {

    // Value
    private String value = null;
    private int round =-1;

    public RoundChangeMessage(ConsensusMessage value, int round) {
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

    public ConsensusMessage deserializePreparedValue() {
        return new Gson().fromJson(this.value, ConsensusMessage.class);
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
