package org.jgroups.protocols.jzookeeper.zabCTAdaptive;

public class AckCommitHandler {
	private int numberOfAck = 0;
	private byte type;
	
	
	public AckCommitHandler(byte type) {
		this.type = type;
	}

	public AckCommitHandler(int numberOfAck, byte type) {
		this.numberOfAck = numberOfAck;
		this.type = type;
	}

	public int getNumberOfAck() {
		return numberOfAck;
	}

	public void setNumberOfAck(int numberOfAck) {
		this.numberOfAck = numberOfAck;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}
	
	public void addACK(){
		numberOfAck++;
	}

}
