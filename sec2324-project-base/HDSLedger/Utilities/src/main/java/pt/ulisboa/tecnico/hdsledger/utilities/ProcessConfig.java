package pt.ulisboa.tecnico.hdsledger.utilities;

public class ProcessConfig {
    public ProcessConfig() {}

    private boolean isClient;

    private String hostname;

    private String id;

    private int port;

    private int[] tests;

    public boolean isClient() { return isClient; }

    public int[] getTests() {
        return tests;
    }
    public boolean getTest(int i) {
        if(i-1 >= tests.length) return false;
        return tests[i-1] == 1;
    }

    public int getPort() {
        return port;
    }

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }
}
