package org.jgroups.protocols.jzookeeper.skeen;

import java.util.concurrent.atomic.AtomicLong;

public class LogicalClock {
	
	private AtomicLong logicalClock;

	public LogicalClock() {
		this.logicalClock = new AtomicLong();
	} 
	
	public LogicalClock(AtomicLong logicalClock) {
		this.logicalClock = logicalClock;
	}
	
	public void incLogicalClock() {
		logicalClock.incrementAndGet();
	}
	
	public long incLogicalClock(long clock) {
		setLogicalClock((Math.max(logicalClock.get(), clock)+1));
		return logicalClock.get();
	}

	public long getLogicalClock() {
		return logicalClock.get();
	}

	public void setLogicalClock(long logicalClock) {
		this.logicalClock.set(logicalClock);
	}
	
	
	
	

}
