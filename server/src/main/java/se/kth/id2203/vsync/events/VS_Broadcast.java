package se.kth.id2203.vsync.events;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class VS_Broadcast implements PatternExtractor<Class, KompicsEvent> {

    public final KompicsEvent message;

    public VS_Broadcast(KompicsEvent message) {
        this.message = message;
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
