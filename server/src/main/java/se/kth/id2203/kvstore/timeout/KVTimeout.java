package se.kth.id2203.kvstore.timeout;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;

public class KVTimeout extends Timeout {

	public KVTimeout(SchedulePeriodicTimeout request) {
		super(request);
	}
}
