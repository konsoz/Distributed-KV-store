package se.kth.id2203.gms;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.id2203.broadcast.beb.events.BEB_Broadcast;
import se.kth.id2203.broadcast.beb.events.BEB_Deliver;
import se.kth.id2203.broadcast.beb.ports.BEBPort;
import se.kth.id2203.epfd.events.Restore;
import se.kth.id2203.epfd.events.Suspect;
import se.kth.id2203.epfd.ports.EPFDPort;
import se.kth.id2203.gms.events.GMSInit;
import se.kth.id2203.gms.events.View;
import se.kth.id2203.gms.ports.GMSPort;
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.omega.Omega;
import se.kth.id2203.omega.events.OmegaInit;
import se.kth.id2203.omega.events.Trust;
import se.kth.id2203.omega.ports.OmegaPort;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

import java.util.Set;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class GMS extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(GMS.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    protected final Positive<Network> net = requires(Network.class);
    public final Negative<GMSPort> gmsPort = provides(GMSPort.class);
    protected final Positive<OmegaPort> omegaPort = requires(OmegaPort.class);
    protected final Positive<EPFDPort> epfdPort = requires(EPFDPort.class);
    protected final Positive<BEBPort> broadcastPort = requires(BEBPort.class);
    private long viewId;
    private Set<NetAddress> members;
    private final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    private NetAddress leader = null;
    private Role role;
    private Component omega;

    public GMS(GMSInit gmsInit){
        LOG.debug("GMS Initialized");
        viewId = 0;
        members = ImmutableSet.copyOf(gmsInit.nodes);
        role = Role.WORKER;
        //trigger(new OmegaInit(ImmutableSet.copyOf(gmsInit.nodes)), omegaPort);
        omega = create(Omega.class, new OmegaInit(gmsInit.nodes));
        //connect(omega.getPositive(OmegaPort.class), ((Component)this).getNegative(OmegaPort.class), Channel.TWO_WAY);

        Omega omegadef = (Omega) omega.getComponent();
        connect(omegadef.omegaPort, omegaPort, Channel.TWO_WAY);

        connect(timer, omega.getNegative(Timer.class), Channel.TWO_WAY);
        connect(net, omega.getNegative(Network.class), Channel.TWO_WAY);
        //trigger(new Start(), omega.getControl());
    }

    protected  final Handler<Trust> trustedHandler = new Handler<Trust>() {
        @Override
        public void handle(Trust trusted) {
            LOG.info("GMS: New leader elected: {}", trusted.trusted);
            leader = trusted.trusted;
            if (leader.equals(self)) {
                role = Role.LEADER;
            } else
                role = Role.WORKER;
        }
    };

    protected final Handler<Suspect> suspectHandler = new Handler<Suspect>() {
        @Override
        public void handle(Suspect event) {
            if(event.suspected.equals(leader) && role == Role.WORKER){
                LOG.info("GMS: Worker detected crash of leader");
            }
            if (role == Role.LEADER) {
                LOG.info("GMS: Leader detected crash, broadcasting new view");
                viewId++;
                members.remove(event.suspected);
                View view = new View(members, viewId, self);
                trigger(new BEB_Broadcast(view, members), broadcastPort);
                trigger(view, gmsPort);
            }
        }
    };

    protected  final Handler<Restore> restoreHandler = new Handler<Restore>() {
        @Override
        public void handle(Restore event) {
            //TODO, should processes be added to the view again?
        }
    };

    protected  final Handler<BEB_Deliver> viewHandler = new Handler<BEB_Deliver>() {
        @Override
        public void handle(BEB_Deliver BEBDeliver) {
            LOG.info("GMS: Received new view from leader");
            if(role == Role.WORKER) {
                View view = (View) BEBDeliver.message;
                if (view.leader.equals(leader)) {
                    members = view.members;
                    viewId = view.id;
                    trigger(view, gmsPort);
                }
            }
        }
    };

    {
        subscribe(trustedHandler, omegaPort);
        subscribe(viewHandler, broadcastPort);
        subscribe(suspectHandler, epfdPort);
    }

    public enum Role {
        LEADER, WORKER
    }

}
