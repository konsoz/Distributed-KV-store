package se.kth.id2203.broadcast.beb.events;

import se.kth.id2203.networking.NetAddress;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class BEB_Deliver implements PatternExtractor<Class, KompicsEvent> {

    public final KompicsEvent message;
    public final NetAddress source;

    public BEB_Deliver(KompicsEvent message, NetAddress source) {
        this.message = message;
        this.source = source;
    }

    @Override
    public Class extractPattern() {
        return message.getClass();
    }

    @Override
    public KompicsEvent extractValue() {
        return message;
    }
}
