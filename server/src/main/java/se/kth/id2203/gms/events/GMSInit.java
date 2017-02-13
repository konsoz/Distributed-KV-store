package se.kth.id2203.gms.events;

import se.kth.id2203.gms.GMS;
import se.kth.id2203.networking.NetAddress;
import se.sics.kompics.Init;

import java.util.Set;

public class GMSInit extends Init<GMS> {

	public final Set<NetAddress> nodes;

	public GMSInit(Set<NetAddress> nodes){
		this.nodes = nodes;
	}

}
