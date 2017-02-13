package se.kth.id2203.kvstore.events;

import se.sics.kompics.KompicsEvent;

import java.util.HashMap;

/**
 * @author Kim Hammar on 2017-02-13.
 */
public class StateTransfer implements KompicsEvent {

    public final HashMap<Integer, String> keyValues;
    public final int sequenceNumber;
    public final int viewId;

    public StateTransfer(HashMap<Integer, String> keyValues, int viewId, int sequenceNumber) {
        this.keyValues = keyValues;
        this.sequenceNumber = sequenceNumber;
        this.viewId = viewId;
    }
}
