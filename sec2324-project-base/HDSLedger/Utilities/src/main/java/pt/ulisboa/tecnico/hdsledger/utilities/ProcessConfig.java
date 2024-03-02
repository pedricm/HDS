package pt.ulisboa.tecnico.hdsledger.utilities;

public class ProcessConfig {
    public ProcessConfig() {}

    private boolean isLeader;
    private boolean isClient;

    private String pubKeyPath;
    private String privKeyPath;

    private String hostname;

    private String id;

    private int port;

    public boolean isLeader() {
        return isLeader;
    }
    public boolean isClient() { return isClient; }

    public int getPort() {
        return port;
    }

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPubKeyPath() { return pubKeyPath; }

    public String getPrivKeyPath() { return privKeyPath; }


}
