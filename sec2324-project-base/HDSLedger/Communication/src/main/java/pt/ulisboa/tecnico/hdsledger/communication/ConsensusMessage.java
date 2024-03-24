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

    // Valid Messages Quorum
    private String validQ = null;
    // Digital Signature (Base64)
    private String DS;

    public ConsensusMessage(String senderId, Type type) {
        super(senderId, type);
    }
    public String getValidQ() {
        return validQ;
    }
    public void setValidQ(ArrayList<ConsensusMessage> validQ) {
        if(validQ == null) {
                this.validQ = null;
                return;
        }
        this.validQ = ObjToString.objToString(validQ);
    }
    public ArrayList<ConsensusMessage> deserializeValidQ() {
        if(this.validQ == null) return null;
        return (ArrayList<ConsensusMessage>) ObjToString.stringToObj(this.validQ);
    }

    public String toJson() {
        return new Gson().toJson(this);
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
    public RoundChangeMessage deserializeRoundChangeMessage() {
        return new Gson().fromJson(this.message, RoundChangeMessage.class);
    }
    public ClientMessage deserializeClientMessage() {
        if(this.message==null) return null;
        return new Gson().fromJson(this.message, ClientMessage.class);
    }
    public ClientResponseMessage deserializeClientResponseMessage() {
        if(this.message==null) return null;
        return new Gson().fromJson(this.message, ClientResponseMessage.class);
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
    /*@Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj.getClass() != this.getClass()) return false;
        ConsensusMessage other = (ConsensusMessage) obj;
        if(this.replyTo == null){
            if (other.getReplyTo() != null) return false;
        }
        else if(!this.replyTo.equals(other.getReplyTo())) return false;

        if(this.message == null){
            if (other.getMessage() != null) return false;
        }
        else if(!this.message.equals(other.getMessage())) return false;

        if(this.validQ == null){
            if (other.getValidQ() != null) return false;
        }
        else if(!this.validQ.equals(other.getValidQ())) return false;

        if(this.DS == null){
            if (other.getDS() != null) return false;
        }
        else if(!this.DS.equals(other.getDS())) return false;

        if(this.consensusInstance != other.getConsensusInstance() || this.round != other.getRound() ||
              this.replyToMessageId != other.getReplyToMessageId()) return false;

        if(!super.equals(other)) return false;
        return true;
    }*/
    public String toString(int ident) {
        String tabs = "";
        for(int i =0; i<ident ;i++) tabs+="\t";
        String stringClass = getClass().getSimpleName() + "{\n"+tabs+"\t" + this.deserializeClientMessage().toString(this.getSenderId(), ident+1);

        stringClass += tabs+"}\n";
        return stringClass;
    }
}
