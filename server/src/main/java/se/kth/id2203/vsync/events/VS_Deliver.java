package se.kth.id2203.vsync.events;

import se.kth.id2203.networking.NetAddress;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class VS_Deliver implements PatternExtractor<Class, KompicsEvent> {

    public final KompicsEvent message;
    public final NetAddress source;
    public long viewId;

    public VS_Deliver(KompicsEvent message, NetAddress source, long viewId) {
        this.message = message;
        this.source = source;
        this.viewId = viewId;
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
