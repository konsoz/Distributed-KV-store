package se.kth.id2203.kvstore.events;

import se.kth.id2203.kvstore.KVService;
import se.kth.id2203.networking.NetAddress;
import se.sics.kompics.Init;

import java.util.Set;

/**
 * @author Kim Hammar on 2017-02-13.
 */
public class KVServiceInit extends Init<KVService> {
    public final Set<NetAddress> replicationGroup;

    public KVServiceInit(Set<NetAddress> replicationGroup) {
        this.replicationGroup = replicationGroup;
    }
}
