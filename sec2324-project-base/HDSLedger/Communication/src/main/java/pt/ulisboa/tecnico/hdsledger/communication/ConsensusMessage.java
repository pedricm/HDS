package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary;

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
    // Digital Signature (Base64)
    private String DS;

    public ConsensusMessage(String senderId, Type type) {
        super(senderId, type);
    }
    public String getDS() {
        return DS;
    }
    public void setDS(String keysPath) {
        this.DS = null;
        this.DS = CryptoLibrary.CreateDS(this, keysPath + "key_" + this.getSenderId() + "_priv.key");
    }
    public Boolean checkDS(String keypath) {
        String DScopy = this.DS;
        this.DS = null;
        Boolean check = CryptoLibrary.Check(this, DScopy, keypath + this.getSenderId() + ".priv");
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
