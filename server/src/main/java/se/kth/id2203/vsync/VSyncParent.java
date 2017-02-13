package se.kth.id2203.vsync;

import se.kth.id2203.broadcast.beb.BEB;
import se.kth.id2203.broadcast.beb.ports.BEBPort;
import se.kth.id2203.epfd.EPFD;
import se.kth.id2203.epfd.events.EPFDInit;
import se.kth.id2203.epfd.ports.EPFDPort;
import se.kth.id2203.gms.GMS;
import se.kth.id2203.gms.events.GMSInit;
import se.kth.id2203.gms.ports.GMSPort;
import se.kth.id2203.omega.Omega;
import se.kth.id2203.omega.events.OmegaInit;
import se.kth.id2203.omega.ports.OmegaPort;
import se.kth.id2203.vsync.events.VSyncInit;
import se.kth.id2203.vsync.events.VSyncParentInit;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 * @author Kim Hammar on 2017-02-13.
 */
public class VSyncParent extends ComponentDefinition {

    protected final Positive<Network> net = requires(Network.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    protected final Component epfd;
    protected final Component omega;
    protected final Component beb;
    protected final Component gms;
    public final Component vSync;

    public VSyncParent(VSyncParentInit init) {
        epfd = create(EPFD.class, new EPFDInit(init.nodes));
        omega = create(Omega.class, new OmegaInit(init.nodes));
        beb = create(BEB.class, Init.NONE);
        gms = create(GMS.class, new GMSInit(init.nodes));
        vSync = create(VSyncService.class, new VSyncInit(init.nodes));

        connect(epfd.getPositive(EPFDPort.class), omega.getNegative(EPFDPort.class), Channel.TWO_WAY);
        connect(epfd.getNegative(Network.class), net, Channel.TWO_WAY);
        connect(epfd.getNegative(Timer.class), timer, Channel.TWO_WAY);

        connect(omega.getPositive(OmegaPort.class), gms.getNegative(OmegaPort.class), Channel.TWO_WAY);

        connect(beb.getPositive(BEBPort.class), vSync.getNegative(BEBPort.class), Channel.TWO_WAY);

        connect(gms.getPositive(GMSPort.class), vSync.getNegative(GMSPort.class), Channel.TWO_WAY);
    }



}
