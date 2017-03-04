package org.saliya.ndssl.multilinearscan;

import java.nio.IntBuffer;

/**
 * Saliya Ekanayake on 3/3/17.
 */
public class VertexBuffer {
    // The real offset would be 2+(offsetFactor*msgSize)
    int offsetFactor;
    IntBuffer buffer;


    public VertexBuffer() {
    }

    public VertexBuffer(int offsetFactor, IntBuffer buffer) {
        this.offsetFactor = offsetFactor;
        this.buffer = buffer;
    }

    public int getOffsetFactor() {
        return offsetFactor;
    }

    public void setOffsetFactor(int offset) {
        this.offsetFactor = offset;
    }

    public IntBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(IntBuffer buffer) {
        this.buffer = buffer;
    }
}
