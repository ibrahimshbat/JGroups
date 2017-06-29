package org.jgroups.protocols.jzookeeper.zabNRWEDCC;

import org.jgroups.util.Bits;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Message Information that is associated with SCInteraction responses.
 *
 * @author Ryan Emerson and Ibrahim EL-Sanosi
 * @since 4.0
 */
public class MessageOrderInfo implements Comparable<MessageOrderInfo>, SizeStreamable {
    private MessageId id = null;
    private long ordering = -1; // Sequence provided by the Zab*** to emulated Infinspan clients.

    public MessageOrderInfo() {
    }
    
    public MessageOrderInfo(long ordering) {
 	   this.ordering = ordering;
     }

    public MessageOrderInfo(MessageId id) {
    	   this.id = id;
    }

    public MessageOrderInfo(MessageId id, long ordering) {
        this.id = id;
        this.ordering = ordering;
    }
    

    public MessageId getId() {
        return id;
    }

    public void setId(MessageId id) {
        this.id = id;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }
   
    @Override
    public int size() {
        return (id != null ? id.size(): 0) + Bits.size(ordering);
      }

    @Override
    public void writeTo(DataOutput out) throws Exception {
        writeMessageId(id, out);
        Bits.writeLong(ordering, out);
    }

    @Override
    public void readFrom(DataInput in) throws Exception {
        id = readMessageId(in);
        ordering = Bits.readLong(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageOrderInfo that = (MessageOrderInfo) o;
        if (ordering != that.ordering) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (ordering ^ (ordering >>> 32));
        return result;
    }

    @Override
    public int compareTo(MessageOrderInfo other) {
        if (this.equals(other))
            return 0;
        else if (ordering > other.ordering)
            return 1;
        else
            return -1;
    }

    @Override
    public String toString() {
        return "MessageInfo{" +
                "id=" + id +
                ", ordering=" + ordering +
                '}';
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



    
    
}