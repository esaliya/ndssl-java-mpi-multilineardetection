package org.saliya.ndssl.multilinearscan.mpi;

import mpi.MPI;

import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class Message {
    private int originalVertexLabel;
    private int msgSize;
    private int[][] data;
    private IntBuffer serializedBytes = null;

    public Message() {

    }

    public Message(int originalVertexLabel){
        this.originalVertexLabel = originalVertexLabel;
    }


    public void copyTo(IntBuffer buffer, int offset){
        buffer.position(offset);
        serializedBytes.position(0);
        buffer.put(serializedBytes);
    }

    public void loadFrom(IntBuffer buffer, int offset, int recvdMsgSize){
        this.msgSize = recvdMsgSize;
        int dimA = buffer.get(offset);
        int dimB = buffer.get(offset+1);
        data = new int[dimA][dimB];
        IntStream.range(0, dimA).forEach(i->IntStream.range(0,dimB).forEach(j->{
            data[i][j] = buffer.get(offset+2+(i*dimB+j));
        }));
    }

    public void pack(){
        serializedBytes.position(0);
        serializedBytes.put(data.length);
        serializedBytes.put((data.length > 0) ? data[0].length : 0);
        for (int[] arr : data){
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

    public int[][] getData() {
        return data;
    }

    public void setDataAndMsgSize(int[][] data, int msgSize) {
        this.data = data;
        // +2 to store dimensions
        this.msgSize = msgSize+2;

        if (serializedBytes == null || serializedBytes.capacity() < this.msgSize){
            serializedBytes = MPI.newIntBuffer(this.msgSize);
        }
    }

    public StringBuffer toString(StringBuffer sb) {
        sb.append("[");
        for(int[] arr : data){
            sb.append(Arrays.toString(arr));
        }
        sb.append("]");
        return sb;
    }
}
