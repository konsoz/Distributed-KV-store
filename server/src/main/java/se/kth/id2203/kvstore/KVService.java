/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.id2203.broadcast.beb.events.BEB_Broadcast;
import se.kth.id2203.gms.events.View;
import se.kth.id2203.kvstore.OpResponse.Code;
import se.kth.id2203.kvstore.events.KVServiceInit;
import se.kth.id2203.kvstore.events.StateTransfer;
import se.kth.id2203.kvstore.events.UpdateAcc;
import se.kth.id2203.kvstore.ports.KVPort;
import se.kth.id2203.kvstore.timeout.KVTimeout;
import se.kth.id2203.networking.Message;
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.overlay.ports.Routing;
import se.kth.id2203.vsync.VSyncService;
import se.kth.id2203.vsync.events.VS_Deliver;
import se.kth.id2203.vsync.events.VSyncInit;
import se.kth.id2203.vsync.ports.VSyncPort;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

import java.util.*;


/**
 * ServiceComponent that handles the actual operation-requests from clients and return results.
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class KVService extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(KVService.class);
    //******* Ports ******
    protected final Positive<Timer> timer = requires(Timer.class);
    protected final Positive<Network> net = requires(Network.class);
    protected final Positive<Routing> route = requires(Routing.class);
    protected final Positive<VSyncPort> vSyncPort = requires(VSyncPort.class);
    public final Negative<KVPort> kvPort = provides(KVPort.class);
    //******* Fields ******
    final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    final HashMap<Integer, String> keyValues = new HashMap<>();
    private View replicationGroup;
    private Set<StateTransfer> unstableUpdates = new HashSet<>();
    private Map<Integer, Set<UpdateAcc>> acknowledgements = new HashMap();
    private UUID timeoutId;
    private int sequenceNumber = 0;
    Component vSync;
    //******* Handlers ******

    public KVService(KVServiceInit init) {
        LOG.debug("KVService Initialized");
        vSync = create(VSyncService.class, new VSyncInit(init.replicationGroup));
        VSyncService vSyncServicedef = (VSyncService) vSync.getComponent();
        connect(vSyncServicedef.vSyncPort, vSyncPort, Channel.TWO_WAY);
        connect(timer, vSync.getNegative(Timer.class), Channel.TWO_WAY);
        connect(net, vSync.getNegative(Network.class), Channel.TWO_WAY);
        //trigger(new Start(), vSync.getControl());
        //trigger(new VSyncInit(ImmutableSet.copyOf(replicationInit.nodes)), vSyncPort);
    }

    protected final Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start start) {
            LOG.debug("KVService Started");
            keyValues.put("1".hashCode(), "first");
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(4000, 4000);
            spt.setTimeoutEvent(new KVTimeout(spt));
            trigger(spt, timer);
            timeoutId = spt.getTimeoutEvent().getTimeoutId();
        }
    };

    protected final Handler<KVTimeout> kvTimeoutHandler = new Handler<KVTimeout>() {
        @Override
        public void handle(KVTimeout kvTimeout) {
            for (StateTransfer stateTransfer : unstableUpdates) {
                if (stateTransfer.sequenceNumber == sequenceNumber) {
                    if (acknowledgements.get(sequenceNumber).size() == replicationGroup.members.size()) {
                        keyValues.clear();
                        keyValues.putAll(stateTransfer.keyValues);
                    }
                }
            }

        }
    };

    /**
     * Operation request received, perform operation and return result.
     */
    protected final ClassMatchedHandler<Operation, Message> opHandler = new ClassMatchedHandler<Operation, Message>() {
        @Override
        public void handle(Operation content, Message context) {
            LOG.info("Got operation {}! Now implement me please :)", content);
            switch (content.operationCode) {
                case GET:
                    if (replicationGroup.leader.equals(self))
                        trigger(new Message(self, context.getSource(), new OpResponse(content.id, Code.OK, keyValues.get(content.key.hashCode()))), net);
                    else {
                        trigger(new Message(self, replicationGroup.leader, content), net);
                    }
                case PUT:
                    if (replicationGroup.leader.equals(self))
                        trigger(new BEB_Broadcast(content, replicationGroup.members), vSyncPort);
                    else {
                        trigger(new Message(self, replicationGroup.leader, content), net);
                    }
                default:
                    trigger(new Message(self, context.getSource(), new OpResponse(content.id, Code.NOT_IMPLEMENTED)), net);
            }
        }
    };

    protected final Handler<View> viewHandler = new Handler<View>() {
        @Override
        public void handle(View view) {
            LOG.debug("KVService recieved new view from VSyncService");
            replicationGroup = view;
        }
    };

    protected final ClassMatchedHandler<UpdateAcc, VS_Deliver> accHandler = new ClassMatchedHandler<UpdateAcc, VS_Deliver>() {
        @Override
        public void handle(UpdateAcc acc, VS_Deliver vs_deliver) {
            if (acknowledgements.containsKey(acc.sequenceNumber)) {
                Set<UpdateAcc> accs = acknowledgements.get(acc.sequenceNumber);
                accs.add(acc);
            }
        }
    };

    protected final ClassMatchedHandler<StateTransfer, VS_Deliver> stateTransferHandler = new ClassMatchedHandler<StateTransfer, VS_Deliver>() {
        @Override
        public void handle(StateTransfer stateTransfer, VS_Deliver vs_deliver) {
            if (vs_deliver.viewId == replicationGroup.id) {
                keyValues.clear();
                keyValues.putAll(stateTransfer.keyValues);
            }
        }
    };
/*
    protected final Handler<ReplicationInit> replicationInitHandler = new Handler<ReplicationInit>() {
        @Override
        public void handle(ReplicationInit replicationInit) {
            LOG.info("KVService initializes replication group");
            trigger(new VSyncInit(ImmutableSet.copyOf(replicationInit.nodes)), vSyncPort);
        }
    };
*/
    /**
     * Kompics "instance initializer", subscribe handlers to ports.
     */ {
        subscribe(opHandler, net);
        subscribe(startHandler, control);
        subscribe(viewHandler, vSyncPort);
        subscribe(accHandler, vSyncPort);
        subscribe(stateTransferHandler, vSyncPort);
        subscribe(kvTimeoutHandler, timer);
    }

}
