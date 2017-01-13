package org.jgroups.protocols.jzookeeper.zabCT_AdaptSwitching;

public class AckHandler {
	private int numberOfAck = 0;
	
	
	public AckHandler(int numberOfAck) {
		this.numberOfAck = numberOfAck;
	}

	public int getNumberOfAck() {
		return numberOfAck;
	}

	public void setNumberOfAck(int numberOfAck) {
		this.numberOfAck = numberOfAck;
	}
	public void addACK(){
		numberOfAck++;
	}

}
