package pt.ulisboa.tecnico.hdsledger.service.models;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ConcurrentHashMap;

import pt.ulisboa.tecnico.hdsledger.communication.CommitMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.PrepareMessage;
import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;

public class MessageBucket {

    private static final CustomLogger LOGGER = new CustomLogger(MessageBucket.class.getName());
    // Quorum size
    private final int quorumSize;

    // Node count
    private final int numberNodes;

    // Instance -> Round -> Sender ID -> Consensus message
    private final Map<Integer, Map<Integer, Map<String, ConsensusMessage>>> bucket = new ConcurrentHashMap<>();
    private final  ArrayList<ConsensusMessage> lastQuorum = new ArrayList<>();

    public MessageBucket(int nodeCount) {
        int f = Math.floorDiv(nodeCount - 1, 3);
        numberNodes = nodeCount;
        quorumSize = Math.floorDiv(nodeCount + f, 2) + 1;
    }

    /*
     * Add a message to the bucket
     * 
     * @param consensusInstance
     * 
     * @param message
     */
    public void addMessage(ConsensusMessage message) {
        int consensusInstance = message.getConsensusInstance();
        int round = message.getRound();

        bucket.putIfAbsent(consensusInstance, new ConcurrentHashMap<>());
        bucket.get(consensusInstance).putIfAbsent(round, new ConcurrentHashMap<>());
        bucket.get(consensusInstance).get(round).put(message.getSenderId(), message);
    }
    public int getQuorumSize(){
        return this.quorumSize;
    }
    public Optional<String> hasValidPrepareQuorum(String nodeId, int instance, int round) {
        // Create mapping of value to frequency
        HashMap<String, Integer> frequency = new HashMap<>();
        HashMap<String, List<ConsensusMessage>> valueToMessages = new HashMap<>();
        bucket.get(instance).get(round).values().forEach((message) -> {
            PrepareMessage prepareMessage = message.deserializePrepareMessage();
            String value = prepareMessage.getValue();
            frequency.put(value, frequency.getOrDefault(value, 0) + 1);
            valueToMessages.computeIfAbsent(value, k -> new ArrayList<>()).add(message);

        });

        // Only one value (if any, thus the optional) will have a frequency
        // greater than or equal to the quorum size
        Optional<String> validValue = frequency.entrySet().stream().filter((Map.Entry<String, Integer> entry) -> {
            return entry.getValue() >= quorumSize;
        }).map((Map.Entry<String, Integer> entry) -> {
            return entry.getKey();
        }).findFirst();
        if(validValue.isPresent()) {
            this.lastQuorum.clear();
            this.lastQuorum.addAll(valueToMessages.get(validValue.get()));
        }
        return validValue;
    }
    public Optional<ArrayList<ConsensusMessage>> getLastQuorum() {
        if (this.lastQuorum.isEmpty()) return Optional.empty();
        return Optional.of(this.lastQuorum);
    }

    public Optional<String> hasValidCommitQuorum(String nodeId, int instance, int round) {
        // Create mapping of value to frequency
        HashMap<String, Integer> frequency = new HashMap<>();
        bucket.get(instance).get(round).values().forEach((message) -> {
            CommitMessage commitMessage = message.deserializeCommitMessage();
            String value = commitMessage.getValue();
            frequency.put(value, frequency.getOrDefault(value, 0) + 1);
        });

        // Only one value (if any, thus the optional) will have a frequency
        // greater than or equal to the quorum size
        return frequency.entrySet().stream().filter((Map.Entry<String, Integer> entry) -> {
            return entry.getValue() >= quorumSize;
        }).map((Map.Entry<String, Integer> entry) -> {
            return entry.getKey();
        }).findFirst();
    }
    public Optional<ArrayList<ConsensusMessage>> getValidCommitQuorum(String nodeId, int instance, int round) {
        // Create mapping of value to frequency
        HashMap<String, Integer> frequency = new HashMap<>();
        HashMap<String, ArrayList<ConsensusMessage>> valueToMessages = new HashMap<>();
        bucket.get(instance).get(round).values().forEach((message) -> {
            CommitMessage commitMessage = message.deserializeCommitMessage();
            String value = commitMessage.getValue();
            frequency.put(value, frequency.getOrDefault(value, 0) + 1);
            valueToMessages.computeIfAbsent(value, k -> new ArrayList<>()).add(message);
        });

        // Only one value (if any, thus the optional) will have a frequency
        // greater than or equal to the quorum size
        Optional<String> validValue = frequency.entrySet().stream().filter((Map.Entry<String, Integer> entry) -> {
            return entry.getValue() >= quorumSize;
        }).map((Map.Entry<String, Integer> entry) -> {
            return entry.getKey();
        }).findFirst();

        if(validValue.isPresent()) {
            return Optional.of(valueToMessages.get(validValue.get()));
        }
        return Optional.empty();
    }

    public Optional<ArrayList<ConsensusMessage>> hasValidRoundChangeQuorum(String nodeId, int instance, int round) {
        // Create mapping of value to frequency
        HashMap<Integer, Integer> frequency = new HashMap<>();
        HashMap<Integer, List<ConsensusMessage>> roundToMessages = new HashMap<>();
        bucket.get(instance).values().forEach((mapMessages) -> {
            mapMessages.values().forEach((message) -> {
                int message_round = message.getRound();
                if (message_round >= round) {
                    frequency.put(message_round, frequency.getOrDefault(message_round, 0) + 1);
                    roundToMessages.computeIfAbsent(message_round, k -> new ArrayList<>()).add(message);
                }
            });
        });

        // Only one value (if any, thus the optional) will have a frequency
        // greater than or equal to the quorum size
        Optional<Integer> validRound = frequency.entrySet().stream().filter((Map.Entry<Integer, Integer> entry) -> {
            return entry.getValue() >= quorumSize;
        }).map((Map.Entry<Integer, Integer> entry) -> {
            return entry.getKey();
        }).findFirst();

        if(validRound.isPresent()) {
            this.lastQuorum.clear();
            this.lastQuorum.addAll(roundToMessages.get(validRound.get()));
            return Optional.of(this.lastQuorum);
        }
        return Optional.empty();
    }

    public Optional<Integer> hasRoundChange(String nodeId, int instance, int round) {
        int f = Math.floorDiv(this.numberNodes - 1, 3);
        HashMap<Integer, Integer> frequency = new HashMap<>();
        bucket.get(instance).values().forEach((mapMessages) -> {
            mapMessages.values().forEach((message) -> {
                int message_round = message.getRound();
                if (message_round > round) {
                    frequency.put(message_round, frequency.getOrDefault(message_round, 0) + 1);
                }
        });
        });


        return frequency.entrySet().stream().filter((Map.Entry<Integer, Integer> entry) -> {
            return entry.getValue() >= f + 1;
        }).map((Map.Entry<Integer, Integer> entry) -> {
            return entry.getKey();
        }).min(Integer::compare);
    }

    public Map<String, ConsensusMessage> getMessages(int instance, int round) {
        return bucket.get(instance).get(round);
    }
}