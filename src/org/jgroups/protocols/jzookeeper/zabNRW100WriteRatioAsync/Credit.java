package org.jgroups.protocols.jzookeeper.zabNRW100WriteRatioAsync;

public class Credit {
	private int counter;

	
	public Credit(int counter) {
		this.counter = counter;
	}

	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}
	
	public int incAndGet(){
		counter++;
		return counter;
	}
	
	public int decAndGet(){
		counter--;
		return counter;
	} 
	
	

}
