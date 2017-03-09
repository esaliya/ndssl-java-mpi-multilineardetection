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
    private short[][] data;
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
        int dimA = buffer.get(offset);
        int dimB = buffer.get(offset+1);
        data = new short[dimA][dimB];
        IntStream.range(0, dimA).forEach(i->IntStream.range(0,dimB).forEach(j->{
            try {
                data[i][j] = buffer.get(offset+2+(i*dimB+j));
            } catch (IndexOutOfBoundsException e) {
                System.out.println("Rank: " + ParallelOps.worldProcRank + " IOOBE idx: " + (offset+2+(i*dimB+j)) + " " +
                        "buff.capacity " + buffer.capacity());
            }
        }));
    }

    public void pack(){
        serializedBytes.position(0);
        serializedBytes.put((short)data.length);
        serializedBytes.put((short)((data.length > 0) ? data[0].length : 0));
        for (short[] arr : data){
            serializedBytes.put(arr);
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

    public short[][] getData() {
        return data;
    }

    public void setDataAndMsgSize(short[][] data, int msgSize) {
        this.data = data;
        // +2 to store dimensions
        this.msgSize = msgSize+2;

        if (serializedBytes == null || serializedBytes.capacity() < this.msgSize){
            serializedBytes = MPI.newShortBuffer(this.msgSize);
        }
    }

    public StringBuffer toString(StringBuffer sb) {
        sb.append("[");
        for(short[] arr : data){
            sb.append(Arrays.toString(arr));
        }
        sb.append("]");
        return sb;
    }
}
