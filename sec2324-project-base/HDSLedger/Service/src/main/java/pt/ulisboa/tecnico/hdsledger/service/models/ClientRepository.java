package pt.ulisboa.tecnico.hdsledger.service.models;

import java.util.Map;
import java.util.Queue;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import pt.ulisboa.tecnico.hdsledger.communication.ConsensusMessage;

public class ClientRepository {
    private final Map<String, Map<Integer, ConsensusMessage>> waitList = new ConcurrentHashMap<>();
    private final Map<String, HashSet<Integer>> doneList = new ConcurrentHashMap<>();

    public ClientRepository() {
    }
    /*
    *  usar quando recebemos algo do client e possivelmente quando preprepare
    * */
    public void addMessage(ConsensusMessage message) {
        String client = message.getSenderId();
        int messageId = message.getMessageId();

        waitList.putIfAbsent(client, new ConcurrentHashMap<>());
        waitList.get(client).put(messageId, message);
    }
    /*
    *  usar quando commit
    *  usar tambem se um node tentar comecar a fazer uma msg que ja foi processada ele vai receber nacks (basta f+1 nao precisa de um quorum)
    *  temos de ter outra estrutura para manter a ordem das msg que recebemos?
    * */
    public void messageDone(String client, int messageId) {
        doneList.putIfAbsent(client, new HashSet<Integer>());
        doneList.get(client).add(messageId);
        /*
        * Quando remove aqui se remove diff de null, remove da queue
        * */
        waitList.putIfAbsent(client, new ConcurrentHashMap<>());
        if(waitList.get(client).remove(messageId) != null) {
            /*
            * funcao que passa pela queue inteira e remove o correto pair <client,msgId>
            * */
        }
    }

}