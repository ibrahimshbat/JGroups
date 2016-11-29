package org.jgroups.protocols.jzookeeper.testrw;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Util;

public class ZabHeader extends Header {
	public static final byte REQUESTR = 1;
	public static final byte REQUESTW = 2;
	public static final byte FORWARD = 3;
	public static final byte PROPOSAL = 4;
	public static final byte ACK = 5;
	public static final byte COMMIT = 6;
	public static final byte RESPONSEW = 7;
	public static final byte RESPONSER = 8;
	public static final byte DELIVER = 9;
	public static final byte COMMITOUTSTANDINGREQUESTS = 10;
	public static final byte RESET = 11;
	public static final byte STATS = 12;
	public static final byte COUNTMESSAGE = 13;
	public static final byte SENDMYADDRESS = 14;
	public static final byte CLIENTFINISHED = 15;

	private byte type = 0;
	private MessageInfo messageInfo = null;
	private Proposal proposal = null;

	public ZabHeader() {
	}
	
	public ZabHeader(byte type) {
		this.type = type;
	}
	
	
	public ZabHeader(MessageInfo messageInfo) {
		this.messageInfo = messageInfo;
	}
	
	public ZabHeader(byte type, MessageInfo messageInfo) {
		this.type = type;
		this.messageInfo = messageInfo;
	}

	public ZabHeader(byte type, Proposal proposal) {
		this.type = type;
		this.proposal = proposal;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}
	

	public MessageInfo getMessageInfo() {
		return messageInfo;
	}

	public void setMessageInfo(MessageInfo messageInfo) {
		this.messageInfo = messageInfo;
	}

	public Proposal getProposal() {
		return proposal;
	}

	public void setProposal(Proposal proposal) {
		this.proposal = proposal;
	}	

	@Override
	public void writeTo(DataOutput out) throws Exception {
		out.writeByte(type);
        Util.writeStreamable(messageInfo, out);
		Util.writeStreamable(proposal, out);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		type = in.readByte();
		messageInfo = new MessageInfo();
        messageInfo = (MessageInfo) Util.readStreamable(MessageInfo.class, in); 
		proposal = new Proposal();
		proposal = (Proposal) Util.readStreamable(Proposal.class, in);
	}

	@Override
	public int size() {
		return Global.BYTE_SIZE + (messageInfo != null ? messageInfo.size() : 0) + (proposal != null ? proposal.size() : 0);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(64);
		sb.append(printType());
		if (proposal != null)
			sb.append(", messageInfo=" + messageInfo);
		if (proposal != null)
			sb.append(", proposal=" + proposal);
		return sb.toString();
	}

	protected final String printType() {

		switch (type) {
		case REQUESTR:
			return "REQUESTR";
		case REQUESTW:
			return "REQUESTW";
		case FORWARD:
			return "FORWARD";
		case PROPOSAL:
			return "PROPOSAL";
		case ACK:
			return "ACK";
		case COMMIT:
			return "COMMIT";
		case RESPONSER:
			return "RESPONSER";
		case RESPONSEW:
			return "RESPONSEW";
		case DELIVER:
			return "DELIVER";
		case COMMITOUTSTANDINGREQUESTS:
			return "COMMITOUTSTANDINGREQUESTS";
		case RESET:
			return "RESET";
		case STATS:
			return "STATS";
		case COUNTMESSAGE:
			return "COUNTMESSAGE";
		case SENDMYADDRESS:
			return "SENDMYADDRESS";
		case CLIENTFINISHED:
			return "CLIENTFINISHED";
		default:
			return "n/a";
		}
	}

}