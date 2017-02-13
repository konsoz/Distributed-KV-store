package se.kth.id2203.vsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.id2203.broadcast.beb.events.BEB_Broadcast;
import se.kth.id2203.broadcast.beb.events.BEB_Deliver;
import se.kth.id2203.broadcast.beb.ports.BEBPort;
import se.kth.id2203.gms.GMS;
import se.kth.id2203.gms.events.GMSInit;
import se.kth.id2203.gms.events.View;
import se.kth.id2203.gms.ports.GMSPort;
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.vsync.events.VS_Broadcast;
import se.kth.id2203.vsync.events.VS_Deliver;
import se.kth.id2203.vsync.events.VSyncInit;
import se.kth.id2203.vsync.ports.VSyncPort;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class VSyncService extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(VSyncService.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    protected final Positive<Network> net = requires(Network.class);
    public final Negative<VSyncPort> vSyncPort = provides(VSyncPort.class);
    protected final Positive<GMSPort> gmsPort = requires(GMSPort.class);
    protected final Positive<BEBPort> broadcastPort = requires(BEBPort.class);
    final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    private View currentView;
    private Component gms;

    public VSyncService(VSyncInit vSyncInit){
        LOG.debug("VSyncService initialized");
        gms = create(GMS.class, new GMSInit(vSyncInit.nodes));
        GMS gmsdef = (GMS) gms.getComponent();
        connect(gmsdef.gmsPort, gmsPort, Channel.TWO_WAY);
        connect(timer, gms.getNegative(Timer.class), Channel.TWO_WAY);
        connect(net, gms.getNegative(Network.class), Channel.TWO_WAY);
        //trigger(new Start(), gms.getControl());
//        Component kvService = create(KVService.class, new KVServiceInit((Set) replicationGroup));
//        connect(kvService.getPositive(KVPort.class), (Negative) kvPort, Channel.TWO_WAY);
//        trigger(new Start(), kvService.getControl());
        //trigger(new GMSInit(ImmutableSet.copyOf(vSyncInit.nodes)), gmsPort);
    }


    protected final Handler<VS_Broadcast> broadcastHandler = new Handler<VS_Broadcast>() {
        @Override
        public void handle(VS_Broadcast vs_broadcast) {
            trigger(new VS_Deliver(vs_broadcast.message, self, currentView.id), vSyncPort);
            trigger(new BEB_Broadcast(vs_broadcast.message, currentView.members), broadcastPort);
        }
    };

    protected final Handler<BEB_Deliver> deliverHandler = new Handler<BEB_Deliver>() {
        @Override
        public void handle(BEB_Deliver BEBDeliver) {
            trigger(new VS_Deliver(BEBDeliver.message, BEBDeliver.source, currentView.id), vSyncPort);
        }
    };

    protected final Handler<View> viewHandler = new Handler<View>() {
        @Override
        public void handle(View view) {
            LOG.debug("VSync received new view from GMS");
            currentView = view;
            trigger(view, vSyncPort);
        }
    };

    {
        subscribe(viewHandler, gmsPort);
        subscribe(deliverHandler, broadcastPort);
        subscribe(broadcastHandler, vSyncPort);
    }


}
