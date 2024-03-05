package pt.ulisboa.tecnico.hdsledger.utilities;

import java.util.TimerTask;

public class InstanceTimerTask extends TimerTask {

    public final int consensusInstanceTimer;


    public InstanceTimerTask ( int consensusInstanceTimer )
    {
      this.consensusInstanceTimer = consensusInstanceTimer;
    }
    @Override
    public void run() {
    }
}