package org.saliya.ndssl.multilinearscan.mpi;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class RecvVertexBuffer extends VertexBuffer {
    int recvfromRank;
    int msgSizeOffset;

    public RecvVertexBuffer(int offsetFactor, ShortBuffer buffer, int recvfromRank, int msgSizeOffset) {
        super(offsetFactor, buffer);
        this.recvfromRank = recvfromRank;
        this.msgSizeOffset = msgSizeOffset;
    }

    public int getMessageSize(){
        return buffer.get(msgSizeOffset);
    }
}
