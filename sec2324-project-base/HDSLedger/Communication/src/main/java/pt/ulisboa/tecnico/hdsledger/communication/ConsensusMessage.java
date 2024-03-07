package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary;
import pt.ulisboa.tecnico.hdsledger.utilities.ObjToString;
import java.util.ArrayList;

public class ConsensusMessage extends Message {

    // Consensus instance
    private int consensusInstance;
    // Round
    private int round;
    // Who sent the previous message
    private String replyTo;
    // Id of the previous message
    private int replyToMessageId;
    // Message (PREPREPARE, PREPARE, COMMIT)
    private String message;

    // Client message
    private ConsensusMessage client;
    // Valid Messages Quorum
    private String validQ = null;
    // Digital Signature (Base64)
    private String DS;
    private int preparedRound = -1;
    private String preparedValue;

    public ConsensusMessage(String senderId, Type type) {
        super(senderId, type);
    }
    public String getValidQ() {
        return validQ;
    }
    public void setValidQ(ArrayList<ConsensusMessage> validQ) {
        if(validQ == null) this.validQ = null;
        this.validQ = ObjToString.objToString(validQ);
    }
    public ArrayList<ConsensusMessage> deserializeValidQ() {
        if(this.validQ == null) return null;
        return (ArrayList<ConsensusMessage>) ObjToString.stringToObj(this.validQ);
    }
    public int getPreparedRound() {
        return preparedRound;
    }

    public void setPreparedRound(int preparedRound) {
        this.preparedRound = preparedRound;
    }

    public String getPreparedValue() {
        return preparedValue;
    }

    public void setPreparedValue(String preparedValue) {
        this.preparedValue = preparedValue;
    }

    public ConsensusMessage getClient() {
        return client;
    }

    public void setClient(ConsensusMessage client) {
        this.client = client;
    }

    public String getDS() {
        return DS;
    }
    public void setDS(String keysPath) {
        this.DS = null;

        if(this.getSenderId() == null) return;
        this.DS = CryptoLibrary.CreateDS(this, keysPath + "key_" + this.getSenderId() + "_priv.key");
    }
    public Boolean checkDS(String keypath) {
        String DScopy = this.DS;
        this.DS = null;
        if(this.getSenderId() == null) return false;
        Boolean check = CryptoLibrary.Check(this, DScopy, keypath + "key_" + this.getSenderId() + "_pub.key");
        this.DS = DScopy;
        return check;
    }
    public PrePrepareMessage deserializePrePrepareMessage() {
        return new Gson().fromJson(this.message, PrePrepareMessage.class);
    }

    public PrepareMessage deserializePrepareMessage() {
        return new Gson().fromJson(this.message, PrepareMessage.class);
    }

    public CommitMessage deserializeCommitMessage() {
        return new Gson().fromJson(this.message, CommitMessage.class);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getConsensusInstance() {
        return consensusInstance;
    }

    public void setConsensusInstance(int consensusInstance) {
        this.consensusInstance = consensusInstance;
    }

    public int getRound() {
        return round;
    }

    public void setRound(int round) {
        this.round = round;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public int getReplyToMessageId() {
        return replyToMessageId;
    }

    public void setReplyToMessageId(int replyToMessageId) {
        this.replyToMessageId = replyToMessageId;
    }
}
