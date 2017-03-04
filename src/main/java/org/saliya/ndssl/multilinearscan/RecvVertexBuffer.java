package org.saliya.ndssl.multilinearscan;

import java.nio.IntBuffer;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class RecvVertexBuffer extends VertexBuffer {
    int recvfromRank;

    public RecvVertexBuffer(int offsetFactor, IntBuffer buffer, int recvfromRank) {
        super(offsetFactor, buffer);
        this.recvfromRank = recvfromRank;
    }
}
