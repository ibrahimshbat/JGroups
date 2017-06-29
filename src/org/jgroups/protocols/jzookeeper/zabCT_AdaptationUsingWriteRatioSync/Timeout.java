package org.jgroups.protocols.jzookeeper.zabCT_AdaptationUsingWriteRatioSync;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;

public class Timeout implements Comparable<Timeout>,  SizeStreamable {
	
	private MessageId id=null;
	private long sendTime=0;
	private long arriveTime=0;
	private long sendBackTimeout=0;
	private long arriveTimeForRoundTrip=0;
	
	
	public Timeout() {

	}

	public Timeout(long sendTime) {
		this.sendTime = sendTime;
	}
	

//	public String getZxid() {
//		return zxid;
//	}
//
//	public void setZxid(String zxid) {
//		this.zxid = zxid;
//	}

	public long getSendTime() {
		return sendTime;
	}

	public void setSendTime(long sendTime) {
		this.sendTime = sendTime;
	}

	public long getArriveTime() {
		return arriveTime;
	}

	public void setArriveTime(long arriveTime) {
		this.arriveTime = arriveTime;
	}

	
	public long getSendBackTimeout() {
		return sendBackTimeout;
	}

	public void setSendBackTimeout(long sendBackTimeout) {
		this.sendBackTimeout = sendBackTimeout;
	}

	public long getArriveTimeForRoundTrip() {
		return arriveTimeForRoundTrip;
	}

	public void setArriveTimeForRoundTrip(long arriveTimeForRoundTrip) {
		this.arriveTimeForRoundTrip = arriveTimeForRoundTrip;
	}
	
	

	public MessageId getId() {
		return id;
	}

	public void setId(MessageId id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Timeout other = (Timeout) obj;
		if (!id.equals(other.id))
			return false;
		return true;
	}
	
	@Override
	public int compareTo(Timeout t){
		
		if(this.id.compareTo(t.getId())>0)
			return 1;
		if(this.id.compareTo(t.getId())<0)
			return -1;		
		else
		return 0;
	}
	
	

	@Override
	public String toString() {
		return "Timeout [id=" + id + ", sendTime=" + sendTime + ", arriveTime=" + arriveTime + "]";
	}
	
	
	 @Override
	    public void writeTo(DataOutput out) throws Exception {
	        //Bits.writeString(id, out);
	        Util.writeStreamable(id, out);
	        Bits.writeLong(sendTime, out);
	        Bits.writeLong(arriveTime, out);
	        Bits.writeLong(sendBackTimeout, out);
	        Bits.writeLong(arriveTimeForRoundTrip, out);	        
	    }

	    @Override
	    public void readFrom(DataInput in) throws Exception {
	        //zxid = Bits.readString(in);
			id = (MessageId) Util.readStreamable(MessageId.class, in); 
	        sendTime = Bits.readLong(in);
	        arriveTime= Bits.readLong(in);
	        sendBackTimeout= Bits.readLong(in);
	        arriveTimeForRoundTrip=Bits.readLong(in);
	    }

		@Override
		public int size() {
	        return (id==null?0:id.size()) + Bits.size(sendTime) +
	        		Bits.size(arriveTime) + Bits.size(sendBackTimeout) + Bits.size(arriveTimeForRoundTrip);
		}
	
	
	
	
	

}
