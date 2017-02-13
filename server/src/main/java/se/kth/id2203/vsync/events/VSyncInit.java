package se.kth.id2203.vsync.events;

import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.vsync.VSyncService;
import se.sics.kompics.Init;

import java.util.Set;

public class VSyncInit extends Init<VSyncService> {

	public final Set<NetAddress> nodes;

	public VSyncInit(Set<NetAddress> nodes){
		this.nodes = nodes;
	}

}
