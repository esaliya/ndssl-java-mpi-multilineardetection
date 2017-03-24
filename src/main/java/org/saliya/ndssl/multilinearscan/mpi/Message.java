package org.saliya.ndssl.multilinearscan.mpi;

import mpi.MPI;

import java.nio.ShortBuffer;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class Message {
    private int originalVertexLabel;
    private int msgSize;
    private short[] data;
    private ShortBuffer serializedBytes = null;

    public Message() {

    }

    public Message(int originalVertexLabel){
        this.originalVertexLabel = originalVertexLabel;
    }


    public void copyTo(ShortBuffer buffer, int offset){
        buffer.position(offset);
        serializedBytes.position(0);
        buffer.put(serializedBytes);
    }

    public void loadFrom(ShortBuffer buffer, int offset, int recvdMsgSize){
        this.msgSize = recvdMsgSize;
        int dim = buffer.get(offset);
        data = new short[dim];
        IntStream.range(0, dim).forEach(i-> data[i] = buffer.get(offset+1+i));
    }

    public void pack(){
        serializedBytes.position(0);
        serializedBytes.put((short)data.length);
        for (short i : data){
            serializedBytes.put(i);
        }
    }

    public int getOriginalVertexLabel() {
        return originalVertexLabel;
    }

    public void setOriginalVertexLabel(int originalVertexLabel) {
        this.originalVertexLabel = originalVertexLabel;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public short[] getData() {
        return data;
    }

    public void setDataAndMsgSize(short[] data, int msgSize) {
        this.data = data;
        // +2 to store dimensions
        this.msgSize = msgSize+1;

        if (serializedBytes == null || serializedBytes.capacity() < this.msgSize){
            serializedBytes = MPI.newShortBuffer(this.msgSize);
        }
    }

    public StringBuffer toString(StringBuffer sb) {
        sb.append("[ ");
        for(short i : data){
            sb.append(" " + i);
        }
        sb.append("]");
        return sb;
    }
}
