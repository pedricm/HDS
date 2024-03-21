package pt.ulisboa.tecnico.hdsledger.communication;

import com.google.gson.Gson;
import pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary;
import pt.ulisboa.tecnico.hdsledger.utilities.ObjToString;
import java.util.ArrayList;
import pt.ulisboa.tecnico.hdsledger.communication.TransactionMessage;
import java.io.Serializable;

public class BlockMessage implements Serializable {

    // Value
    private String transactions;

    private String leaderId;

    private String DS;

    public BlockMessage(ArrayList<TransactionMessage> transactions, String leaderId) {
        this.setTransactions(transactions);
        this.leaderId = leaderId;
    }

    public String getTransactions() {
        return transactions;
    }

    public void setTransactions(ArrayList<TransactionMessage> transactions) {
        if(transactions == null) {
            this.transactions = null;
            return;
        }
        this.transactions = ObjToString.objToString(transactions);
    }
    public ArrayList<TransactionMessage> deserializeTransactions() {
        if(this.transactions == null) return null;
        return (ArrayList<TransactionMessage>) ObjToString.stringToObj(this.transactions);
    }
    public void setDS(String keysPath) {
        this.DS = null;

        if(this.leaderId == null) return;
        this.DS = CryptoLibrary.CreateDS(this, keysPath + "key_" + this.leaderId + "_priv.key");
    }
    public Boolean checkDS(String keypath) {
        String DScopy = this.DS;
        this.DS = null;
        if(this.leaderId == null) return false;
        Boolean check = CryptoLibrary.Check(this, DScopy, keypath + "key_" + this.leaderId + "_pub.key");
        this.DS = DScopy;
        return check;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }
}
