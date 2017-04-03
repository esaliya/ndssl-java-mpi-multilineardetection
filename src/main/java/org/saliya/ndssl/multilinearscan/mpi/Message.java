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
    private short data;

    public Message() {

    }

    public Message(short data){
        this.data = data;
    }

    public void copyTo(ShortBuffer buffer, int offset){
        buffer.put(offset, data);
    }

    public void loadFrom(ShortBuffer buffer, int offset){
        this.data = buffer.get(offset);
    }


    public void setData(short data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return String.valueOf(data);
    }

    public int getMsgSize() {
        return 1;
    }
}
