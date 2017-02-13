package se.kth.id2203.kvstore.events;

import se.sics.kompics.KompicsEvent;

/**
 * @author Kim Hammar on 2017-02-13.
 */
public class UpdateAcc implements KompicsEvent {

    public final int sequenceNumber;

    public UpdateAcc(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}
