package se.kth.id2203.gms.ports;

import se.kth.id2203.gms.events.View;
import se.sics.kompics.PortType;

/**
 * @author Kim Hammar on 2017-02-08.
 */
public class GMSPort extends PortType {

    {
        indication(View.class);
    }
}