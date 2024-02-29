package pt.ulisboa.tecnico.hdsledger.communication.builder;

import pt.ulisboa.tecnico.hdsledger.communication.ClientMessage;
import pt.ulisboa.tecnico.hdsledger.communication.Message;

public class ClientMessageBuilder {
    private final ClientMessage instance;

    public ClientMessageBuilder(String sender, Message.Type type) {
        instance = new ClientMessage(sender, type);
    }

    public ClientMessageBuilder setMessage(String message) {
        instance.setMessage(message);
        return this;
    }

   public ClientMessageBuilder setReplyTo(String replyTo) {
        instance.setReplyTo(replyTo);
        return this;
   }

    public ClientMessageBuilder setReplyToMessageId(int replyToMessageId) {
        instance.setReplyToMessageId(replyToMessageId);
        return this;
    }

    public ClientMessage build() {
        return instance;
    }
}
