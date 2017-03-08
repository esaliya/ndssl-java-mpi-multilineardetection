package org.saliya.ndssl.multilinearscan.mpi;

import java.nio.IntBuffer;
import java.nio.ShortBuffer;

/**
 * Saliya Ekanayake on 3/3/17.
 */
public class VertexBuffer {
    // The real offset would be 2+(offsetFactor*msgSize)
    int offsetFactor;
    ShortBuffer buffer;


    public VertexBuffer() {
    }

    public VertexBuffer(int offsetFactor, ShortBuffer buffer) {
        this.offsetFactor = offsetFactor;
        this.buffer = buffer;
    }

    public int getOffsetFactor() {
        return offsetFactor;
    }

    public void setOffsetFactor(int offset) {
        this.offsetFactor = offset;
    }

    public ShortBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ShortBuffer buffer) {
        this.buffer = buffer;
    }
}
