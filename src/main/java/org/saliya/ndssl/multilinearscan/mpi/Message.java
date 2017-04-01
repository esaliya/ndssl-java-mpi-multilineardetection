package org.saliya.ndssl.multilinearscan.mpi;

import mpi.MPI;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class Message {
    private int originalVertexLabel;
    private int msgSize;
    private short[] data;
    private ShortBuffer serializedBytes = null;
    private short dimA;
    private short dimB;
    private int readOffset;
    private ShortBuffer buffer;

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

    /*public void loadFrom(ShortBuffer buffer, int offset, int recvdMsgSize){
        this.msgSize = recvdMsgSize;
        int dimA = buffer.get(offset);
        int dimB = buffer.get(offset+1);
        data = new short[dimA][dimB];
        IntStream.range(0, dimA).forEach(i->IntStream.range(0,dimB).forEach(j->{
            data[i][j] = buffer.get(offset+2+(i*dimB+j));
        }));
    }*/

    public void loadFrom(ShortBuffer buffer, int offset, int recvdMsgSize){
        this.msgSize = recvdMsgSize;
        this.dimA = buffer.get(offset);
        this.readOffset = offset+1;
        this.buffer = buffer;
    }

    public short get(int i){
        return buffer.get(readOffset+i);
    }

    public void pack(){
        serializedBytes.position(0);
        serializedBytes.put((short)data.length);
        serializedBytes.position(1);
        serializedBytes.put(data);
//        for (short i : data){
//            serializedBytes.put(i);
//        }
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
