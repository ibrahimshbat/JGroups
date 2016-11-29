package org.jgroups.protocols.jzookeeper.zabstagered;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import java.io.DataInput;
import java.io.DataOutput;

/* Message Information.
 * @author Ryan Emerson and Ibrahim EL-Sanosi
 * @since 4.0
 */
public class MessageInfo implements Comparable<MessageInfo>, SizeStreamable {
	private MessageId id = null;
	private long zxid = -1;
	
	public MessageInfo() {
	}

	public MessageInfo(long zxid) {
		this.zxid = zxid;
	}

	public MessageInfo(MessageId id) {
		this.id = id;
	}

	public MessageInfo(MessageId id, long zxid) {
		this.id = id;
		this.zxid = zxid;

	}

	public MessageId getId() {
		return id;
	}

	public void setId(MessageId id) {
		this.id = id;
	}

	public long getZxid() {
		return zxid;
	}

	public void setZxid(long zxid) {
		this.zxid = zxid;
	}

	@Override
	public void writeTo(DataOutput out) throws Exception {
		writeMessageId(id, out);
		Bits.writeLong(zxid, out);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		id = readMessageId(in);
		zxid = Bits.readLong(in);
	}

	@Override
	public int size() {
		return (id != null ? id.size() : 0) + Bits.size(zxid);
	}

	private void writeMessageId(MessageId id, DataOutput out) throws Exception {
		if (id == null) {
			out.writeShort(-1);
		} else {
			out.writeShort(1);
			id.writeTo(out);
		}
	}

	private MessageId readMessageId(DataInput in) throws Exception {
		short length = in.readShort();
		if (length < 0) {
			return null;
		} else {
			MessageId id = new MessageId();
			id.readFrom(in);
			return id;
		}
	}

	@Override
	public int compareTo(MessageInfo other) {
		if (id.compareTo(other.id) < 0)
			return -1;
		else if (id.compareTo(other.id) > 0)
			return 1;
		else
			return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		MessageInfo that = (MessageInfo) o;
		if (zxid != that.zxid)
			return false;
		if (id != null ? !id.equals(that.id) : that.id != null)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		int result = id != null ? id.hashCode() : 0;
		result = 31 * result + (int) (zxid ^ (zxid >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "MessageInfo{" + "id=" + id + ", zxid =" + zxid + '}';
	}

}