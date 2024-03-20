package pt.ulisboa.tecnico.hdsledger.service.services;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.Timer;
import java.util.TimerTask;
import java.security.PublicKey;
import com.google.gson.Gson;

import pt.ulisboa.tecnico.hdsledger.communication.CommitMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ClientMessage;
import pt.ulisboa.tecnico.hdsledger.communication.Link;
import pt.ulisboa.tecnico.hdsledger.communication.Message;
import pt.ulisboa.tecnico.hdsledger.communication.PrePrepareMessage;
import pt.ulisboa.tecnico.hdsledger.communication.RoundChangeMessage;
import pt.ulisboa.tecnico.hdsledger.communication.ClientResponseMessage;

import pt.ulisboa.tecnico.hdsledger.communication.PrepareMessage;
import pt.ulisboa.tecnico.hdsledger.communication.builder.ConsensusMessageBuilder;
import pt.ulisboa.tecnico.hdsledger.service.models.InstanceInfo;
import pt.ulisboa.tecnico.hdsledger.service.models.MessageBucket;
import pt.ulisboa.tecnico.hdsledger.service.models.ClientRepository;
import pt.ulisboa.tecnico.hdsledger.service.models.Account;
import pt.ulisboa.tecnico.hdsledger.utilities.CustomLogger;
import pt.ulisboa.tecnico.hdsledger.utilities.ProcessConfig;
import pt.ulisboa.tecnico.hdsledger.utilities.InstanceTimerTask;
import pt.ulisboa.tecnico.hdsledger.utilities.CryptoLibrary;


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
    // Consensus instance -> Round -> List of round changes messages
    private final MessageBucket roundchangeMessages;

    // Store if already received pre-prepare for a given <consensus, round>
    private final Map<Integer, Map<Integer, Boolean>> receivedPrePrepare = new ConcurrentHashMap<>();
    // Consensus instance information per consensus instance
    private final Map<Integer, InstanceInfo> instanceInfo = new ConcurrentHashMap<>();
    // Current consensus instance
    private final AtomicInteger consensusInstance = new AtomicInteger(0);
    // Last decided consensus instance
    private final AtomicInteger lastDecidedConsensusInstance = new AtomicInteger(0);

    // Ledger (for now, just a list of strings)
    private ArrayList<ConsensusMessage> ledger = new ArrayList<ConsensusMessage>();

    private Map<Integer, ConsensusMessage> finishedEpochs = new ConcurrentHashMap<>();

    private String keysPath;

    private ClientRepository clientRepository = new ClientRepository();

    private Map<PublicKey, Account> accounts = new ConcurrentHashMap<>();
    public NodeService(Link link, ProcessConfig config, ProcessConfig[] nodesConfig, String keysPath) {

        this.link = link;
        this.config = config;
        this.nodesConfig = nodesConfig;
        this.keysPath = keysPath;
        this.prepareMessages = new MessageBucket(nodesConfig.length);
        this.commitMessages = new MessageBucket(nodesConfig.length);
        this.roundchangeMessages = new MessageBucket(nodesConfig.length);

        //DEFAULT ACCOUNTS
        accounts.put(CryptoLibrary.readPublicKey(keysPath + "key_5_pub.key"), new Account("5", 5000));
        accounts.put(CryptoLibrary.readPublicKey(keysPath + "key_6_pub.key"), new Account("6", 70));
        accounts.put(CryptoLibrary.readPublicKey(keysPath + "key_7_pub.key"), new Account("7", 4000));
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

    public ArrayList<ConsensusMessage> getLedger() {
        return this.ledger;
    }

    private boolean isLeader(int instance, int round, String id) {
        return getLeader(instance, round).equals(id);
    }
 
    public ConsensusMessage createConsensusMessage(ConsensusMessage value, int instance, int round, ArrayList<ConsensusMessage> roundChange) {
        PrePrepareMessage prePrepareMessage = new PrePrepareMessage(value);
        ConsensusMessage consensusMessage;
            /*if (config.getTest(2)) {
                ConsensusMessage ms = clientMessage;
                ms.setMessage("bizantine message");
                consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.PRE_PREPARE)
                        .setConsensusInstance(instance)
                        .setRound(round)
                        .setMessage(prePrepareMessage.toJson())
                        .setClient(ms)
                        .build();*/
        consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.PRE_PREPARE)
                .setConsensusInstance(instance)
                .setRound(round)
                .setMessage(prePrepareMessage.toJson())
                .setValidQ(roundChange)
                .build();


        return consensusMessage;
    }
    public ConsensusMessage createConsensusMessageRoundChange(int prepRound, String prepValue, int instance, int round) {
        Optional<ArrayList<ConsensusMessage>> validQ = prepareMessages.getLastQuorum();
        ConsensusMessage consensusMessage;
        RoundChangeMessage rcm = new RoundChangeMessage(prepValue, prepRound);
        if(validQ.isPresent() && this.instanceInfo.get(instance).getPreparedRound() > 0){
            consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.ROUND_CHANGE)
                    .setConsensusInstance(instance)
                    .setRound(round)
                    .setMessage(rcm.toJson())
                    .setValidQ(validQ.get())
                    .build();

        } else {
            consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.ROUND_CHANGE)
                    .setConsensusInstance(instance)
                    .setRound(round)
                    .setMessage(rcm.toJson())
                    .setValidQ(null)
                    .build();
        }

        return consensusMessage;
    }
    public void createAndSendClientResponseMessage(ConsensusMessage client, ClientMessage cl) {
        ClientResponseMessage clr = new ClientResponseMessage(accounts.get(cl.getPubSource()).getAmount(), true);
        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.ACK_CLIENT)
                .setReplyTo(client.getSenderId())
                .setReplyToMessageId(client.getMessageId())
                .setMessage(clr.toJson())
                .build();
        this.link.send(client.getSenderId(), consensusMessage);
    }
    public void createAndSendClientResponseMessageTransfer(ConsensusMessage client, boolean ack) {
        ClientResponseMessage clr = new ClientResponseMessage(-2, ack);
        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.ACK_CLIENT)
                .setReplyTo(client.getSenderId())
                .setReplyToMessageId(client.getMessageId())
                .setMessage(clr.toJson())
                .build();
        this.link.send(client.getSenderId(), consensusMessage);
    }
    public void checkBalanceOrTransfer(ConsensusMessage client) {
        ClientMessage cl = client.deserializeClientMessage();
        int amount = cl.getAmount();
        // CHECK_BALANCE
        if(cl.getPubDest() == null && amount == -1){
            //Send ACK_CLIENT to client
            createAndSendClientResponseMessage(client, cl);
            System.out.println("AMOUNT: "+ client.getSenderId() +" : "+ client.getMessageId() +" : " + accounts.get(cl.getPubSource()).getAmount()+ "-----------------------------------");
        }
        // TRANSFER
        else {
            if (cl.getPubDest() == null || amount == -1 || !accounts.get(cl.getPubSource()).checkTransaction(amount)){
                createAndSendClientResponseMessageTransfer(client, false);
                System.out.println("FAILED TRANSFER: "+ client.getSenderId() +" : "+ client.getMessageId() +" : " + accounts.get(cl.getPubSource()).getAmount() + " " + accounts.get(cl.getPubDest()).getAmount() + "-----------------------------------");
                return;
            }
            System.out.println("BEFORE TRANSFER: "+ client.getSenderId() +" : "+ client.getMessageId() +" : " + accounts.get(cl.getPubSource()).getAmount() + " " + accounts.get(cl.getPubDest()).getAmount() + "-----------------------------------");
            accounts.get(cl.getPubSource()).transfer(amount);
            accounts.get(cl.getPubDest()).deposit(amount);
            createAndSendClientResponseMessageTransfer(client, true);
            System.out.println("AFTER TRANSFER: "+ client.getSenderId() +" : "+ client.getMessageId() +" : " + accounts.get(cl.getPubSource()).getAmount() + " " + accounts.get(cl.getPubDest()).getAmount() + "-----------------------------------");
        }
    }
    public int  applyTransactionsFromBuffer(int consensusInstance, int times) {
        boolean flag = true;
        while(flag) {
            if (finishedEpochs.get(consensusInstance+times) != null){
                ConsensusMessage client = finishedEpochs.get(consensusInstance+times);
                ledger.ensureCapacity(consensusInstance+times);
                while (ledger.size() < consensusInstance+times - 1) {
                    ledger.add(null);
                }

                ledger.add(consensusInstance+times - 1, client);
                // FAZ COISAS RESPONDE AO CLIENT E FAZ TRANSACTION
                checkBalanceOrTransfer(client);
                finishedEpochs.remove(consensusInstance+times);
                times++;
            } else {
                flag = false;
            }
        }
        return times;
    }
    public Timer createTimerTask(int localConsensusInstance, Timer timer){
        TimerTask task = new InstanceTimerTask(localConsensusInstance) {
            @Override
            public void run() {
                // FIX PARA O CANCEL : se o lastinstance > this.consensusIntance cancelar este timer

                InstanceInfo instance = instanceInfo.get(this.consensusInstanceTimer);
                if (instance.getCommittedRound() != -1){
                    instance.cancelTimer();
                    return;
                }
                instance.setCurrentRound(instance.getCurrentRound()+1);
                link.broadcast(createConsensusMessageRoundChange(instance.getPreparedRound(), instance.getPreparedValue(), this.consensusInstanceTimer, instance.getCurrentRound()));
            }
        };
        if(timer != null){
            timer.cancel();
            timer.purge();
        }
        timer = new Timer();
        timer.scheduleAtFixedRate(task, 10000, 10000); // pode ser *roundNumber
        return timer;
    }

    public boolean clientMessageCheck(ConsensusMessage message) {
        ClientMessage value = message.deserializeClientMessage();
        // MESSAGE CHECKS
        if (value == null) return false;
        if (value.getPubSource() == null) return false;
        // CHECK PUBSOURCE
        Account source = this.accounts.get(value.getPubSource());
        if(source == null) return false;
        if(!source.getId().equals(message.getSenderId())) return false;

        if (value.getPubDest() != null){
            if (value.getAmount() <= 0) return false;
            // CHECK PUBDST
            if(this.accounts.get(value.getPubDest()) == null) return false;
        }
        if (value.getPubDest() == null && value.getAmount() != -1) return false;
        return true;
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
        if(config.getTest(4)) return;
        // Set initial consensus values
        // MESSAGE CHECKS
        if(!clientMessageCheck(message))return;

        int localConsensusInstance = this.consensusInstance.incrementAndGet();
        InstanceInfo existingConsensus = this.instanceInfo.put(localConsensusInstance, new InstanceInfo(message));

        // If startConsensus was already called for a given round
        if (existingConsensus != null) {
            LOGGER.log(Level.INFO, MessageFormat.format("{0} - Node already started consensus for instance {1}",
                    config.getId(), localConsensusInstance));
            return;
        }

        InstanceInfo instance = this.instanceInfo.get(localConsensusInstance);
        instance.setTimer(createTimerTask(localConsensusInstance, null));
        // Leader broadcasts PRE-PREPARE message
        if (this.isLeader(localConsensusInstance, instance.getCurrentRound(), this.config.getId())) {
            LOGGER.log(Level.INFO,
                MessageFormat.format("{0} - Node is leader, sending PRE-PREPARE message", config.getId()));
            if (config.getTest(3)) {
                return;
            }
            this.link.broadcast(this.createConsensusMessage(message, localConsensusInstance, instance.getCurrentRound(), null));
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
        if (round < 1 || consensusInstanceMessage < 1 || message.deserializePrePrepareMessage().getValue() == null) return;
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }
        // Verify if pre-prepare was sent by leader
        if (!isLeader(consensusInstanceMessage, round, senderId))
            return;
        /*if (clientRepository.checkIfDone(message.getClient().getSenderId(), message.getClient().getMessageId())){
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with repeated messageId",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }*/
        // CHECKS VALUE
        ConsensusMessage clientMessage = message.deserializePrePrepareMessage().deserializeValue();
        if (clientMessage == null || !clientMessage.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS of client message",
                            config.getId(), senderId, consensusInstanceMessage, round));
            return;
        }

        // MESSAGE CHECKS
        if(!clientMessageCheck(clientMessage))return;

        if (message.getValidQ() == null) {
            if(message.getRound() != 1) return;
        } else {
            if (message.getRound() == 1) return;
            //JUSTIFY PREPREPARE
            ArrayList<ConsensusMessage> validQ = message.deserializeValidQ();
            final int[] cmround = {-1};
            final int size = roundchangeMessages.getQuorumSize();
            final String[] cmVal = {null};
            // At least size of quorum
            if (validQ.size() != size) return;
            // Justify preprepare j2
            validQ.stream().forEach( cm -> {
                if (cm.getRound() < 1 || cm.getConsensusInstance() < 1) return;
                if (!cm.checkDS(this.keysPath)) {
                    LOGGER.log(Level.INFO,
                            MessageFormat.format("{0} - Received PRE-PREPARE JUSTIFICATION - ROUND-CHANGE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                                    config.getId(), message.getSenderId(), consensusInstance, round));
                    return;
                }
                RoundChangeMessage rcm = cm.deserializeRoundChangeMessage();
                if (rcm.getPreparedRound() > 0) {
                    if(cm.getValidQ() == null || rcm.getPreparedValue() == null) return;
                    cm.deserializeValidQ().stream().forEach( qm -> {
                        if (qm.getRound() != rcm.getPreparedRound() || !qm.deserializePrepareMessage().getValue().equals(rcm.getPreparedValue()) || !qm.checkDS(this.keysPath)) {
                            LOGGER.log(Level.INFO,
                                    MessageFormat.format("{0} - Received PRE-PREPARE JUSTIFICATION - PREPARE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                                            config.getId(), message.getSenderId(), consensusInstance, round));
                            return;
                        }
                    });
                    if(rcm.getPreparedRound() > cmround[0]) {
                        cmround[0] = rcm.getPreparedRound();
                        cmVal[0] = rcm.getPreparedValue();
                    }
                } else if (rcm.getPreparedRound() != -1 || rcm.getPreparedValue() != null || cm.getValidQ() != null) {
                    return;
                }
            });
            if (cmround[0] != -1 && !message.deserializePrePrepareMessage().getValue().equals(cmVal[0])) {
                return;
            }
        }

        LOGGER.log(Level.INFO,
                MessageFormat.format(
                        "{0} - Received PRE-PREPARE message from {1} Consensus Instance {2}, Round {3}",
                        config.getId(), senderId, consensusInstanceMessage, round));

        // Set instance value
        if (this.instanceInfo.putIfAbsent(consensusInstanceMessage, new InstanceInfo(null)) == null) {
            this.instanceInfo.get(consensusInstanceMessage).setTimer(createTimerTask(consensusInstanceMessage, null));
        }
        InstanceInfo ii = this.instanceInfo.get(consensusInstanceMessage);
        if (ii != null && ii.getCurrentRound() != round)
            ii.setCurrentRound(round);

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
        PrepareMessage prepareMessage;
        if(config.getTest(5)){
            ConsensusMessage biz = message.deserializePrePrepareMessage().deserializeValue();
            ClientMessage cl = biz.deserializeClientMessage();
            cl.setAmount(100000000);
            biz.setMessage(cl.toJson());
            prepareMessage = new PrepareMessage(biz.toJson());
        }
        else prepareMessage = new PrepareMessage(message.deserializePrePrepareMessage().getValue());

        ConsensusMessage consensusMessage = new ConsensusMessageBuilder(config.getId(), Message.Type.PREPARE)
                .setConsensusInstance(consensusInstanceMessage)
                .setRound(round)
                .setMessage(prepareMessage.toJson())
                .setReplyTo(senderId)
                .setReplyToMessageId(senderMessageId)
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
        if (round < 1 || consensusInstance < 1) return;
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PREPARE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), senderId, consensusInstance, round));
            return;
        }

        // CHECKS VALUE
        /*ConsensusMessage clientMessage = message.deserializePrepareMessage().deserializeValue();
        if (clientMessage == null || !clientMessage.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Received PREPARE message from {1} Consensus Instance {2}, Round {3} with faulty DS of client message",
                            config.getId(), senderId, consensusInstance, round));
            return;
        }
        // MESSAGE CHECKS
        if(!clientMessageCheck(clientMessage))return;*/

        LOGGER.log(Level.INFO,
                MessageFormat.format(
                        "{0} - Received PREPARE message from {1}: Consensus Instance {2}, Round {3}",
                        config.getId(), senderId, consensusInstance, round));

        // Doesn't add duplicate messages
        prepareMessages.addMessage(message);


        // TESTAR TAMBEM SE E DUPLICADO MSG CLIENT
        // TODO
        // Set instance values
        if (this.instanceInfo.putIfAbsent(consensusInstance, new InstanceInfo(null)) == null) {
            this.instanceInfo.get(consensusInstance).setTimer(createTimerTask(consensusInstance, null));
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

        if(config.getTest(6) && instanceInfo.get(consensusInstance).getCurrentRound() == 1) return;
        /*
         *  Check msg DS
         *
         * */
        if (round < 1 || consensusInstance < 1) return;
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Received COMMIT message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), message.getSenderId(), consensusInstance, round));
            return;
        }

        String validQs = message.getValidQ();
        if(config.getTest(7) && validQs == null) return;

        if(validQs != null) {
            List<ConsensusMessage> validQ = message.deserializeValidQ();
            String[] msValue = {null};
            validQ.stream().forEach(ms -> {
                if (msValue[0] == null) msValue[0] = ms.deserializeCommitMessage().getValue();

                if (round != ms.getRound() || consensusInstance != ms.getConsensusInstance() || !msValue[0].equals(ms.deserializeCommitMessage().getValue()) || !ms.checkDS(this.keysPath)) {
                    LOGGER.log(Level.INFO,
                            MessageFormat.format("{0} - Received ?COMMIT QUORUM SUB? message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                                    config.getId(), ms.getSenderId(), ms.getConsensusInstance(), ms.getRound()));
                    return;
                }
            });

            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Received COMMIT QUORUM message from {1}: Consensus Instance {2}, Round {3}",
                            config.getId(), message.getSenderId(), consensusInstance, round));

            validQ.stream().forEach( ms -> {
                commitMessages.addMessage(ms);
            });
        } else {
            // CHECKS VALUE
            /*ConsensusMessage clientMessage = message.deserializeCommitMessage().deserializeValue();
            if (clientMessage == null || !clientMessage.checkDS(this.keysPath)) {
                LOGGER.log(Level.INFO,
                        MessageFormat.format(
                                "{0} - Received COMMIT message from {1} Consensus Instance {2}, Round {3} with faulty DS of client message",
                                config.getId(), message.getSenderId(), consensusInstance, round));
                return;
            }
            // MESSAGE CHECKS
            if(!clientMessageCheck(clientMessage))return;*/
            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Received COMMIT message from {1}: Consensus Instance {2}, Round {3}",
                            config.getId(), message.getSenderId(), consensusInstance, round));

            commitMessages.addMessage(message);
        }

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
        // TODO NESTA E NA OUTRA DO COMMIT, VER SE VALE A PENA VERIFICAR SE MSGNUMBER IGUAL
        Optional<String> commitValue = commitMessages.hasValidCommitQuorum(config.getId(),
                consensusInstance, round);

        if (commitValue.isPresent() && instance.getCommittedRound() < round) {

            instance = this.instanceInfo.get(consensusInstance);
            instance.setCommittedRound(round);

            ConsensusMessage value = new Gson().fromJson(commitValue.get(), ConsensusMessage.class);
            int times = 0;
            // Append value to the ledger (must be synchronized to be thread-safe)
            synchronized(ledger) {
                if(consensusInstance != lastDecidedConsensusInstance.get()+1){
                    finishedEpochs.put(consensusInstance, value);
                    LOGGER.log(Level.INFO,
                            MessageFormat.format(
                                    "{0} - Added to instance buffer: ({1}, {2} ,{3})",
                                    config.getId(), consensusInstance, value.getSenderId(), value.getMessageId()));
                    synchronized(lastDecidedConsensusInstance) {
                        int consensusInstanceLast = lastDecidedConsensusInstance.get() + 1;
                        times = applyTransactionsFromBuffer(consensusInstanceLast, times);
                    }
                } else {
                    // Increment size of ledger to accommodate current instance
                    ledger.ensureCapacity(consensusInstance);
                    while (ledger.size() < consensusInstance - 1) {
                        ledger.add(null);
                    }

                    ledger.add(consensusInstance - 1, value);
                    // FAZ COISAS RESPONDE AO CLIENT E FAZ TRANSACTION
                    checkBalanceOrTransfer(value);
                    times++;
                    times = applyTransactionsFromBuffer(consensusInstance,times);
                    //LOGGER.log(Level.INFO,
                    //        MessageFormat.format(
                    //               "{0} - Current Ledger: {1}",
                    //                config.getId(), String.join("", ledger)));
                }
            }
            for (int i = 0; i < times; i++){
                lastDecidedConsensusInstance.getAndIncrement();
            }

            // TODO - tem de se ter f+1 ds iguais para se fazer isso?
            //clientRepository.messageDone(message.getClient().getSenderId(), message.getClient().getMessageId());
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Decided on Consensus Instance {1}, Round {2}, Successful? {3}",
                            config.getId(), consensusInstance, round, true));
        }
    }
    public synchronized void uponRoundChange(ConsensusMessage message) {
        int consensusInstance = message.getConsensusInstance();
        int round = message.getRound();

        /*
         *  Check msg DS
         *
         * */
        if (round < 1 || consensusInstance < 1) return;
        if (!message.checkDS(this.keysPath)) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format("{0} - Received ROUND_CHANGE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                            config.getId(), message.getSenderId(), consensusInstance, round));
            return;
        }
        LOGGER.log(Level.INFO,
                MessageFormat.format("{0} - Received ROUND_CHANGE message from {1}: Consensus Instance {2}, Round {3}",
                        config.getId(), message.getSenderId(), consensusInstance, round));
        // Verify (Justify each roundChange message)
        RoundChangeMessage rcm = message.deserializeRoundChangeMessage();
        if(rcm == null) return;
        if (rcm.getPreparedRound() > 0) {
            if(message.getValidQ() == null || rcm.getPreparedValue() == null) return;
            message.deserializeValidQ().stream().forEach( qm -> {
                if (qm.getRound() != rcm.getPreparedRound() || !qm.deserializePrepareMessage().getValue().equals(rcm.getPreparedValue()) || !qm.checkDS(this.keysPath)) {
                    LOGGER.log(Level.INFO,
                            MessageFormat.format("{0} - Received ROUND_CHANGE JUSTIFICATION - PREPARE message from {1}: Consensus Instance {2}, Round {3} with faulty DS",
                                    config.getId(), message.getSenderId(), consensusInstance, round));
                    return;
                }
            });
        } else if (rcm.getPreparedRound() != -1 || rcm.getPreparedValue() != null || message.getValidQ() != null) return;

        roundchangeMessages.addMessage(message);

        /*
        *  Se nós já decidimos esta ronda:
        *  Mandamos o quorum de commits para o sender
        * */
        if (lastDecidedConsensusInstance.get() >= consensusInstance) {
            LOGGER.log(Level.INFO,
                    MessageFormat.format(
                            "{0} - Already COMMITED for Consensus Instance {1}, Round {2}, ignoring",
                            config.getId(), consensusInstance, round));

            Optional<ArrayList<ConsensusMessage>> validQ = commitMessages.getValidCommitQuorum(config.getId(), consensusInstance, this.instanceInfo.get(consensusInstance).getCommittedRound());

            //Send the quorum of commits to the sender
            ConsensusMessage m;
            if(validQ.isPresent()) {

                m = new ConsensusMessageBuilder(config.getId(), Message.Type.COMMIT)
                        .setConsensusInstance(consensusInstance)
                        .setRound((validQ.get()).get(0).getRound())
                        .setReplyTo(message.getSenderId())
                        .setReplyToMessageId(message.getMessageId())
                        .setValidQ(validQ.get())
                        .build();
            } else { // Supostamente nao acontece nunca
                m = new ConsensusMessageBuilder(config.getId(), Message.Type.COMMIT)
                        .setConsensusInstance(consensusInstance)
                        .setReplyTo(message.getSenderId())
                        .setReplyToMessageId(message.getMessageId())
                        .setValidQ(null)
                        .build();
            }
            link.send(message.getSenderId(), m);

            return;
        }

        InstanceInfo instance = this.instanceInfo.get(consensusInstance);
        Optional<Integer> roundChangeAmp;
        if(instance == null) {
            roundChangeAmp = roundchangeMessages.hasRoundChange(config.getId(), consensusInstance, 1);
            if (roundChangeAmp.isPresent()){
                this.instanceInfo.putIfAbsent(consensusInstance, new InstanceInfo(null));
                instance = this.instanceInfo.get(consensusInstance);
            }
            else
                return;
        }
        else {
            roundChangeAmp = roundchangeMessages.hasRoundChange(config.getId(), consensusInstance, instance.getCurrentRound());
        }
        if (roundChangeAmp.isPresent()) {
            instance.setCurrentRound(roundChangeAmp.get());
            instance.setTimer(createTimerTask(consensusInstance, instance.getTimer()));
            //BCAST roundchange
            this.link.broadcast(createConsensusMessageRoundChange(instance.getPreparedRound(), instance.getPreparedValue(), consensusInstance, instance.getCurrentRound()));
        }

        //If we are the leader (for the next round)
        Optional<String[]> cmVal = roundchangeMessages.hasValidRoundChangeQuorum(config.getId(), consensusInstance, instance.getCurrentRound());
        if (cmVal.isPresent() && this.isLeader(consensusInstance, instance.getCurrentRound(), this.config.getId()) && instance.getRoundChangeRound() < instance.getCurrentRound()) {
            instance.setCurrentRound(instance.getCurrentRound());
            Optional<ArrayList<ConsensusMessage>> validQ = roundchangeMessages.getLastQuorum();
            if(!validQ.isPresent()) return; // impossible
            //System.out.println("asssssssssssssssssssssssssssssssssssssssssssssskkkkkkkkkkkkkkkkkksssssssss"+validQ.get().size());

            //ver se tem um Q de prepares por msg ou nada
            if(cmVal.get()[0] == null) {
                // If he does not have an input value, waits for another round change
                if(instance.getInputValue() == null){
                    return;
                }
                this.link.broadcast(this.createConsensusMessage(instance.getInputValue(), consensusInstance, instance.getCurrentRound(), validQ.get()));
            } else {
                ConsensusMessage cm = new Gson().fromJson(cmVal.get()[0], ConsensusMessage.class);
                this.link.broadcast(this.createConsensusMessage(cm, consensusInstance, instance.getCurrentRound(), validQ.get()));
            }
        }
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

                                case ROUND_CHANGE ->
                                    uponRoundChange((ConsensusMessage) message);

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
