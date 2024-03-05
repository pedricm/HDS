package pt.ulisboa.tecnico.hdsledger.service.services;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.Timer;
import java.util.TimerTask;

import pt.ulisboa.tecnico.hdsledger.communication.CommitMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.PrePrepareMessage;
import pt.ulisboa.tecnico.hdsledger.communication.PrepareMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ConsensusMessageBuilder;
import pt.ulisboa.tecnico.hdsledger.service.models.InstanceInfo;
import pt.ulisboa.tecnico.hdsledger.service.models.MessageBucket;
import pt.ulisboa.tecnico.hdsledger.service.models.ClientRepository;
import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.utilities.InstanceTimerTask;


public class NodeService implements UDPService {

    private static final CustomLogger LOGGER = new CustomLogger(NodeService.class.getName());
    // Nodes configurations
    private final ProcessConfig[] nodesConfig;

    // Current node is leader
    private final ProcessConfig config;

    // Link to communicate with nodes
    private final Link link;

    // Consensus instance -> Round -> List of prepare messages
    private final MessageBucket prepareMessages;
    // Consensus instance -> Round -> List of commit messages
    private final MessageBucket commitMessages;

    // Store if already received pre-prepare for a given <consensus, round>
    private final Map<Integer, Map<Integer, Boolean>> receivedPrePrepare = new ConcurrentHashMap<>();
    // Consensus instance information per consensus instance
    private final Map<Integer, InstanceInfo> instanceInfo = new ConcurrentHashMap<>();
    // Current consensus instance
    private final AtomicInteger consensusInstance = new AtomicInteger(0);
    // Last decided consensus instance
    private final AtomicInteger lastDecidedConsensusInstance = new AtomicInteger(0);

    // Ledger (for now, just a list of strings)
    private ArrayList<String> ledger = new ArrayList<String>();

    private String keysPath;

    private ClientRepository clientRepository = new ClientRepository();
    public NodeService(Link link, ProcessConfig config, ProcessConfig[] nodesConfig, String keysPath) {

        this.link = link;
        this.config = config;
        this.nodesConfig = nodesConfig;
        this.keysPath = keysPath;
        this.prepareMessages = new MessageBucket(nodesConfig.length);
        this.commitMessages = new MessageBucket(nodesConfig.length);
    }
    public String getLeader(int instance, int round) {
        int N = nodesConfig.length;
        return  Integer.toString(((instance + round) % N)+1); // HAS TO BE +1
    }
    public ProcessConfig getConfig() {
        return this.config;
    }

    public int getConsensusInstance() {
        return this.consensusInstance.get();
    }

    public ArrayList<String> getLedger() {
        return this.ledger;
    }

    private boolean isLeader(int instance, int round, String id) {
        return getLeader(instance, round).equals(id);
    }
 
    public ConsensusMessage createConsensusMessage(ConsensusMessage clientMessage, String value, int instance, int round) {
        PrePrepareMessage prePrepareMessage = new PrePrepareMessage(value);

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.PRE_PREPARE)
                .setConsensusInstance(instance)
                .setRound(round)
                .setMessage(prePrepareMessage.toJson())
                .setClient(clientMessage)
                .build();

        return consensusMessage;
    }
    public ConsensusMessage createConsensusMessageRoundChange(int prepRound, String prepValue, int instance, int round) {

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.ROUND_CHANGE)
                .setConsensusInstance(instance)
                .setRound(round)
                .setPreparedRound(prepRound)
                .setPreparedValue(prepValue)
                .build();

        return consensusMessage;
    }
    public Timer createTimerTask(int localConsensusInstance){
        TimerTask task = new InstanceTimerTask(localConsensusInstance) {
            @Override
            public void run() {
                // FIX PARA O CANCEL : se o lastinstance > this.consensusIntance cancelar este timer

                InstanceInfo instance = instanceInfo.get(this.consensusInstanceTimer);
                if (lastDecidedConsensusInstance.get() >= this.consensusInstanceTimer ) instance.cancelTimer();

                    System.out.println("aaaaaaaaaaaaa "+this.consensusInstanceTimer +" "+ instance.getPreparedRound()+" " +instance.getPreparedValue()+" "+ instance.getCurrentRound());
                //this.link.broadcast(createConsensusMessageRoundChange(instance.getPreparedRound(), instance.getPreparedValue(), instance.getCurrentRound(), ))
            }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(task, 10000, 10000);
        return timer;
    }
    /*
     * Start an instance of consensus for a value
     * Only the current leader will start a consensus instance
     * the remaining nodes only update values.
     *
     * @param inputValue Value to value agreed upon
     */
    public void startConsensus(ConsensusMessage message) {
        /*
        *  Check msg DS and check if valid string
        *  TODO
        * */
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received APPEND message from {1} with faulty DS",
                            config.getId(), message.getSenderId()));
            return;
        }
        // veri also if it is a client TODO
        /*if (clientRepository.checkIfDone(message.getSenderId(), message.getMessageId())){
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received APPEND message from {1} with a messageId already used",
                            config.getId(), message.getSenderId()));
            return;
        }*/

        // Set initial consensus values
        String value = message.getMessage();
        int localConsensusInstance = this.consensusInstance.incrementAndGet();
        InstanceInfo existingConsensus = this.instanceInfo.put(localConsensusInstance, new InstanceInfo(value));

        // If startConsensus was already called for a given round
        if (existingConsensus != null) {
            LOGGER.log(Level.INFO, MessageFormat.format("{0} - Node already started consensus for instance {1}",
                    config.getId(), localConsensusInstance));
            return;
        }

        // Only start a consensus instance if the last one was decided
        // We need to be sure that the previous value has been decided
        while (lastDecidedConsensusInstance.get() < localConsensusInstance - 1) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        /*if (clientRepository.checkIfDone(message.getSenderId(), message.getMessageId())){
            // change after, esta assim porque queremos ignorar se chegar paralelo enquanto so tem 1 lider e nao tem mudanca
            lastDecidedConsensusInstance.getAndIncrement();
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received APPEND message from {1} with a messageId already used",
                            config.getId(), message.getSenderId()));
            return;
        }*/
        InstanceInfo instance = this.instanceInfo.get(localConsensusInstance);
        instance.setTimer(createTimerTask(localConsensusInstance));
        // Leader broadcasts PRE-PREPARE message
        if (this.isLeader(localConsensusInstance, instance.getCurrentRound(), this.config.getId())) {
            LOGGER.log(Level.INFO,
                MessageFormat.format("{0} - Node is leader, sending PRE-PREPARE message", config.getId()));
            this.link.broadcast(this.createConsensusMessage(message, value, localConsensusInstance, instance.getCurrentRound()));
        } else {
            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Node is not leader, waiting for PRE-PREPARE message", config.getId()));
        }
    }

    /*
     * Handle pre prepare messages and if the message
     * came from leader and is justified them broadcast prepare
     *
     * @param message Message to be handled
     */
    public void uponPrePrepare(ConsensusMessage message) {

        int consensusInstanceMessage = message.getConsensusInstance();
        int round = message.getRound();
        String senderId = message.getSenderId();
        int senderMessageId = message.getMessageId();
        /*
         *  Check msg DS
         *
         * */
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }
        /*if (clientRepository.checkIfDone(message.getClient().getSenderId(), message.getClient().getMessageId())){
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with repeated messageId",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }*/
        if (!message.getClient().checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS of client message",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }
        PrePrepareMessage prePrepareMessage = message.deserializePrePrepareMessage();

        String value = prePrepareMessage.getValue();

        LOGGER.log(Level.INFO,
                MessageFormat.format(
                        "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3}",
                        config.getId(), senderId, consensusInstanceMessage, round));

        // Verify if pre-prepare was sent by leader
        if (!isLeader(consensusInstanceMessage, round, senderId))
            return;
        /*
         * Verify also if correct instance if not return?
         * TODO
         * */
        if (lastDecidedConsensusInstance.get() + 1 != consensusInstanceMessage)
            return;
        InstanceInfo ii = this.instanceInfo.get(consensusInstanceMessage);

        if (ii != null && ii.getCurrentRound() != round)
            return;
        if (ii == null && round != 1)
            return;
        // Set instance value
        if (this.instanceInfo.putIfAbsent(consensusInstanceMessage, new InstanceInfo(value)) == null) {
            this.instanceInfo.get(consensusInstanceMessage).setTimer(createTimerTask(consensusInstanceMessage));
        }

        // Within an instance of the algorithm, each upon rule is triggered at most once
        // for any round r
        receivedPrePrepare.putIfAbsent(consensusInstanceMessage, new ConcurrentHashMap<>());
        if (receivedPrePrepare.get(consensusInstanceMessage).put(round, true) != null) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Already received PRE-PREPARE message for Consensus Instance {1}, Round {2}, "
                                    + "replying again to make sure it reaches the initial sender",
                            config.getId(), consensusInstanceMessage, round));
        }

        PrepareMessage prepareMessage = new PrepareMessage(prePrepareMessage.getValue());

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.PREPARE)
                .setConsensusInstance(consensusInstanceMessage)
                .setRound(round)
                .setMessage(prepareMessage.toJson())
                .setReplyTo(senderId)
                .setReplyToMessageId(senderMessageId)
                .setClient(message.getClient())
                .build();

        this.link.broadcast(consensusMessage);
    }

    /*
     * Handle prepare messages and if there is a valid quorum broadcast commit
     *
     * @param message Message to be handled
     */
    public synchronized void uponPrepare(ConsensusMessage message) {

        int consensusInstance = message.getConsensusInstance();
        int round = message.getRound();
        String senderId = message.getSenderId();
        /*
         *  Check msg DS
         *
         * */
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PREPARE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), senderId, consensusInstance, round));
            return;
        }
        PrepareMessage prepareMessage = message.deserializePrepareMessage();

        String value = prepareMessage.getValue();

        LOGGER.log(Level.INFO,
                MessageFormat.format(
                        "{0} - Received PREPARE message from {1}: Consensus Instance {2}, Round {3}",
                        config.getId(), senderId, consensusInstance, round));

        // Doesn't add duplicate messages
        prepareMessages.addMessage(message);

        // Set instance values

        // TESTAR TAMBEM SE E DUPLICADO MSG CLIENT
        // TODO
        InstanceInfo ii = this.instanceInfo.get(consensusInstance);
        if (ii == null && !message.getClient().checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS of client message",
                            config.getId(), senderId, consensusInstance, round));
            return;
        }

        if (this.instanceInfo.putIfAbsent(consensusInstance, new InstanceInfo(value)) == null) {
            this.instanceInfo.get(consensusInstance).setTimer(createTimerTask(consensusInstance));
        }
        InstanceInfo instance = this.instanceInfo.get(consensusInstance);

        // Within an instance of the algorithm, each upon rule is triggered at most once
        // for any round r
        // Late prepare (consensus already ended for other nodes) only reply to him (as
        // an ACK)
        if (instance.getPreparedRound() >= round) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Already received PREPARE message for Consensus Instance {1}, Round {2}, "
                                    + "replying again to make sure it reaches the initial sender",
                            config.getId(), consensusInstance, round));

            ConsensusMessage m = new ConsensusMessageBuilder(config.getId(), Message.Type.COMMIT)
                    .setConsensusInstance(consensusInstance)
                    .setRound(round)
                    .setReplyTo(senderId)
                    .setReplyToMessageId(message.getMessageId())
                    .setMessage(instance.getCommitMessage().toJson())
                    .setClient(message.getClient())
                    .build();

            link.send(senderId, m);
            return;
        }

        // Find value with valid quorum
        Optional<String> preparedValue = prepareMessages.hasValidPrepareQuorum(config.getId(), consensusInstance, round);
        if (preparedValue.isPresent() && instance.getPreparedRound() < round) {
            instance.setPreparedValue(preparedValue.get());
            instance.setPreparedRound(round);

            // Must reply to prepare message senders
            Collection<ConsensusMessage> sendersMessage = prepareMessages.getMessages(consensusInstance, round)
                    .values();

            CommitMessage c = new CommitMessage(preparedValue.get());
            instance.setCommitMessage(c);

            sendersMessage.forEach(senderMessage -> {
                ConsensusMessage m = new ConsensusMessageBuilder(config.getId(), Message.Type.COMMIT)
                        .setConsensusInstance(consensusInstance)
                        .setRound(round)
                        .setReplyTo(senderMessage.getSenderId())
                        .setReplyToMessageId(senderMessage.getMessageId())
                        .setMessage(c.toJson())
                        .setClient(message.getClient())
                        .build();

                link.send(senderMessage.getSenderId(), m);
            });
        }
    }



    /*
     * Handle commit messages and decide if there is a valid quorum
     *
     * @param message Message to be handled
     */
    public synchronized void uponCommit(ConsensusMessage message) {

        int consensusInstance = message.getConsensusInstance();
        int round = message.getRound();

        /*
         *  Check msg DS
         *
         * */
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Received COMMIT message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), message.getSenderId(), consensusInstance, round));
            return;
        }
        LOGGER.log(Level.INFO,
                MessageFormat.format("{0} - Received COMMIT message from {1}: Consensus Instance {2}, Round {3}",
                        config.getId(), message.getSenderId(), consensusInstance, round));

        commitMessages.addMessage(message);

        InstanceInfo instance = this.instanceInfo.get(consensusInstance);

        if (instance == null) {
            // Should never happen because only receives commit as a response to a prepare message
            MessageFormat.format(
                    "{0} - CRITICAL: Received COMMIT message from {1}: Consensus Instance {2}, Round {3} BUT NO INSTANCE INFO",
                    config.getId(), message.getSenderId(), consensusInstance, round);
            return;
        }

        // Within an instance of the algorithm, each upon rule is triggered at most once
        // for any round r
        if (instance.getCommittedRound() >= round) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Already received COMMIT message for Consensus Instance {1}, Round {2}, ignoring",
                            config.getId(), consensusInstance, round));
            return;
        }

        Optional<String> commitValue = commitMessages.hasValidCommitQuorum(config.getId(),
                consensusInstance, round);
        /*
        * Ver se existe prob em 2 mensagens chegarem ao mesmo tempo ambas entravam no if nao?
        * ASK
        * */
        if (commitValue.isPresent() && instance.getCommittedRound() < round) {

            instance = this.instanceInfo.get(consensusInstance);
            instance.setCommittedRound(round);

            String value = commitValue.get();

            // Append value to the ledger (must be synchronized to be thread-safe)
            synchronized(ledger) {

                // Increment size of ledger to accommodate current instance
                ledger.ensureCapacity(consensusInstance);
                while (ledger.size() < consensusInstance - 1) {
                    ledger.add("");
                }
                
                ledger.add(consensusInstance - 1, value);
                
                LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Current Ledger: {1}",
                            config.getId(), String.join("", ledger)));
            }

            lastDecidedConsensusInstance.getAndIncrement();
            // FIX BUG e possivel alguem nao ter timer e chegar aqui, ver como
            //instance.cancelTimer();
            // TODO - tem de se ter f+1 ds iguais para se fazer isso?
            //clientRepository.messageDone(message.getClient().getSenderId(), message.getClient().getMessageId());

            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Decided on Consensus Instance {1}, Round {2}, Successful? {3}",
                            config.getId(), consensusInstance, round, true));
        }
    }
    public synchronized void uponRoundChange(ConsensusMessage message) {

    }

    @Override
    public void listen() {
        try {
            // Thread to listen on every request
            new Thread(() -> {
                try {
                    while (true) {
                        Message message = link.receive();

                        // Separate thread to handle each message
                        new Thread(() -> {

                            switch (message.getType()) {

                                case PRE_PREPARE ->
                                    uponPrePrepare((ConsensusMessage) message);


                                case PREPARE ->
                                    uponPrepare((ConsensusMessage) message);


                                case COMMIT ->
                                    uponCommit((ConsensusMessage) message);


                                case ACK ->
                                    LOGGER.log(Level.INFO, MessageFormat.format("{0} - Received ACK message from {1}",
                                            config.getId(), message.getSenderId()));

                                case IGNORE ->
                                    LOGGER.log(Level.INFO,
                                            MessageFormat.format("{0} - Received IGNORE message from {1}",
                                                    config.getId(), message.getSenderId()));
                                // case append para iniciar consensus que vem do client
                                case APPEND ->
                                    startConsensus((ConsensusMessage) message);
                                /*
                                * QUANDO receber append e nao for o lider da instancia guardar para ver o tempo que o lider demora depois de receber uma segunda confirmacao do client?? ou algo do genero
                                * Ver se e ness alguem comecar uma nova round se o client fizer split brain
                                *   - a instancia vai fazer o que o lider recebeu (OK)
                                *   - mas os que receberam diferente podem querer mudar depois
                                * TODO REVER
                                * */
                                default ->
                                    LOGGER.log(Level.INFO,
                                            MessageFormat.format("{0} - Received unknown message from {1}",
                                                    config.getId(), message.getSenderId()));

                            }

                        }).start();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
