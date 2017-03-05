package org.saliya.ndssl.multilinearscan;

import java.nio.IntBuffer;

/**
 * Saliya Ekanayake on 3/4/17.
 */
public class Message {
    int msgSize;
    int originalVertexLabel;

    int data;

    public Message() {

    }

    public Message(int originalVertexLabel){
        this.originalVertexLabel = originalVertexLabel;
    }


    public void copyTo(IntBuffer buffer, int offset){
        buffer.put(offset, data);
    }

    public void loadFrom(IntBuffer buffer, int offset, int msgSize){
        data = buffer.get(offset);

    }
}
