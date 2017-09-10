package org.jgroups.protocols.jzookeeper.zabACRWForEDCC200000;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Bits;
import org.jgroups.util.Util;


public class ZabACHeader extends Header {
	public static final byte REQUESTR      = 1;
	public static final byte REQUESTW      = 2;
	public static final byte FORWARD       = 3;
	public static final byte PROPOSAL      = 4;
	public static final byte ACK           = 5;
	public static final byte COMMIT        = 6;
	public static final byte RESPONSER      = 7;
	public static final byte RESPONSEW     = 8;
	public static final byte DELIVER       = 9;
	public static final byte START_SENDING = 10;
	public static final byte COMMITOUTSTANDINGREQUESTS = 11;
	public static final byte RESET = 12;
	public static final byte STATS = 13;
	public static final byte COUNTMESSAGE = 14;
	public static final byte STARTREALTEST = 15;
	public static final byte SENDMYADDRESS = 16;
	public static final byte BUNDLEPROPOSAL = 17;
	public static final byte FINISHED = 18;
	public static final byte STARTWORKLOAD = 19;



	private byte        type=0;
	private long        zxid=0;
	private MessageId   messageId=null;
	private MessageOrderInfo messageOrderInfo = null;
	private Collection<MessageOrderInfo> bundledMsgInfo = null;



	public ZabACHeader() {
	}

	public ZabACHeader(byte type) {
		this.type=type;
	}

	public ZabACHeader(MessageOrderInfo messageOrderInfo) {
		this.messageOrderInfo = messageOrderInfo;
	}

	public ZabACHeader(byte type, MessageId id) {
		this.type=type;
		this.messageId=id;
	}

	public ZabACHeader(byte type, MessageOrderInfo messageOrderInfo) {
		this.type=type;
		this.messageOrderInfo=messageOrderInfo;
	}


	public ZabACHeader(byte type, Collection<MessageOrderInfo> bundledMsgInfo) {
		this.type = type;
		this.bundledMsgInfo = bundledMsgInfo;
	}

	public ZabACHeader(byte type, MessageOrderInfo messageOrderInfo, MessageId messageId) {
		this.type=type;
		this.messageOrderInfo=messageOrderInfo;
		this.messageId=messageId;

	}

	public ZabACHeader(byte type, long seqno, MessageOrderInfo messageOrderInfo, MessageId messageId) {
		this.type=type;
		this.zxid=zxid;
		this.messageOrderInfo=messageOrderInfo;
		this.messageId=messageId;

	}

	public ZabACHeader(byte type, long zxid) {
		this(type);
		this.zxid=zxid;
	}
	public ZabACHeader(byte type, long zxid, MessageId messageId) {
		this(type);
		this.zxid=zxid;
		this.messageId=messageId;
	}

	public ZabACHeader(byte type, long zxid, MessageOrderInfo messageOrderInfo) {
		this(type);
		this.zxid=zxid;
		this.messageOrderInfo=messageOrderInfo;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}


	public String toString() {
		StringBuilder sb=new StringBuilder(64);
		sb.append(printType());
		if(zxid >= 0)
			sb.append(" zxid=" + zxid);
		if(messageId!=null)
			sb.append(", message_id=" + messageId);
		if(messageOrderInfo!=null)
			sb.append(", message_Info=" + messageOrderInfo);
		return sb.toString();
	}

	protected final String printType() {

		switch(type) {
		case REQUESTR:        return "REQUESTR";
		case REQUESTW:        return "REQUESTW";
		case FORWARD:        return "FORWARD";
		case PROPOSAL:       return "PROPOSAL";
		case ACK:            return "ACK";
		case COMMIT:         return "COMMIT";
		case RESPONSER:       return "RESPONSER";
		case RESPONSEW:       return "RESPONSEW";

		case DELIVER:        return "DELIVER";
		case START_SENDING:  return "START_SENDING";
		case COMMITOUTSTANDINGREQUESTS:  return "COMMITOUTSTANDINGREQUESTS";
		case RESET:          return "RESET";
		case STATS:			 return "STATS";
		case COUNTMESSAGE:			 return "COUNTMESSAGE";
		case STARTREALTEST:			 return "STARTREALTEST";
		case SENDMYADDRESS:			 return "SENDMYADDRESS";
		case BUNDLEPROPOSAL:			 return "BUNDLEPROPOSAL";
		case FINISHED:			 return "FINISHED";
		case STARTWORKLOAD:		 return "STARTWORKLOAD";
		default:             return "n/a";
		}
	}

	public long getZxid() {
		return zxid;
	}

	public MessageId getMessageId(){
		return messageId;
	}


	public MessageOrderInfo getMessageOrderInfo() {
		return messageOrderInfo;
	}

	public void setMessageOrderInfo(MessageOrderInfo messageOrderInfo) {
		this.messageOrderInfo = messageOrderInfo;
	}


	public Collection<MessageOrderInfo> getBundledMsgInfo() {
		return bundledMsgInfo;
	}

	private int getBundledSize() {
		int size = 0;
		for (MessageOrderInfo info : bundledMsgInfo)
			size += info.size();
		return size;
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		out.writeByte(type);
		Bits.writeLong(zxid,out);
		Util.writeStreamable(messageId, out);
		Util.writeStreamable(messageOrderInfo, out);
        writeBundledMsgInfo(bundledMsgInfo, out);
		//writeMessageOrderInfo(messageOrderInfo, out);
		//messageId.writeTo(out);
		//out.writeBoolean(flush_ack);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		type=in.readByte();
		zxid=Bits.readLong(in);
		messageId = new MessageId();
		messageId = (MessageId) Util.readStreamable(MessageId.class, in); 
		messageOrderInfo = new MessageOrderInfo();
		messageOrderInfo = (MessageOrderInfo) Util.readStreamable(MessageOrderInfo.class, in); 
        bundledMsgInfo = readBundledMsgInfo(in);

		//messageOrderInfo = readMessageOrderInfo(in);
	}
	
	 private void writeBundledMsgInfo(Collection<MessageOrderInfo> bundledHeaders, DataOutput out) throws Exception{
	        if (bundledHeaders == null) {
	            out.writeShort(-1);
	            return;
	        }

	        out.writeShort(bundledHeaders.size());
	        for (MessageOrderInfo info : bundledHeaders)
	            Util.writeStreamable(info, out);
	    }

	    private Collection<MessageOrderInfo> readBundledMsgInfo(DataInput in) throws Exception {
	        short length = in.readShort();
	        if (length < 0) return null;

	        Collection<MessageOrderInfo> bundledHeaders = new ArrayList<MessageOrderInfo>();
	        for (int i = 0; i < length; i++)
	            bundledHeaders.add((MessageOrderInfo) Util.readStreamable(MessageOrderInfo.class, in));
	        return bundledHeaders;
	    }

	@Override
	public int size() {
		//(messageInfo != null ? messageInfo.size() : 0)
		return Global.BYTE_SIZE + Bits.size(zxid) + (messageId != null ? messageId.size(): 0) +
				(messageOrderInfo != null ? messageOrderInfo.size() : 0) + (bundledMsgInfo != null ? getBundledSize() : 0);
	}
	
	  @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        ZabACHeader that = (ZabACHeader) o;

	        if (type != that.type) return false;
	        if (bundledMsgInfo != null ? !bundledMsgInfo.equals(that.bundledMsgInfo) : that.bundledMsgInfo != null)
	            return false;
	        if (messageOrderInfo != null ? !messageOrderInfo.equals(that.messageOrderInfo) : that.messageOrderInfo != null) return false;

	        return true;
	    }

	    @Override
	    public int hashCode() {
	        int result = (int) type;
	        result = 31 * result + (messageOrderInfo != null ? messageOrderInfo.hashCode() : 0);
	        result = 31 * result + (bundledMsgInfo != null ? bundledMsgInfo.hashCode() : 0);
	        return result;
	    }

}