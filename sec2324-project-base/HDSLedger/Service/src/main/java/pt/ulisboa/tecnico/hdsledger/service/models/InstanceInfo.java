package pt.ulisboa.tecnico.hdsledger.service.models;


import pt.ulisboa.tecnico.hdsledger.communication.CommitMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

import java.util.Timer;
public class InstanceInfo {

    private int currentRound = 1;
    private int preparedRound = -1;
    private int committedRound = -1;
    private int roundChangeRound = -1;
    private String preparedValue = null;
    private CommitMessage commitMessage;
    private ConsensusMessage clientMessage;
    private String inputValue;
    private Timer timer = null;
    public InstanceInfo(String inputValue) {
        this.inputValue = inputValue;
    }
    public InstanceInfo(String inputValue, ConsensusMessage clientMessage) {
        this.inputValue = inputValue;
        this.clientMessage = clientMessage;
    }
    public void cancelTimer() {
        if(this.timer == null) return;
        this.timer.cancel();
    }

    public int getRoundChangeRound() {
        return roundChangeRound;
    }

    public void setRoundChangeRound(int roundChangeRound) {
        this.roundChangeRound = roundChangeRound;
    }

    public ConsensusMessage getClientMessage() {
        return clientMessage;
    }

    public void setClientMessage(ConsensusMessage clientMessage) {
        this.clientMessage = clientMessage;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }
    public Timer getTimer() {
        return timer;
    }
    public int getCurrentRound() {
        return currentRound;
    }

    public void setCurrentRound(int currentRound) {
        this.currentRound = currentRound;
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

    public String getInputValue() {
        return inputValue;
    }

    public void setInputValue(String inputValue) {
        this.inputValue = inputValue;
    }

    public int getCommittedRound() {
        return committedRound;
    }

    public void setCommittedRound(int committedRound) {
        this.committedRound = committedRound;
    }

    public CommitMessage getCommitMessage() {
        return commitMessage;
    }

    public void setCommitMessage(CommitMessage commitMessage) {
        this.commitMessage = commitMessage;
    }
}
