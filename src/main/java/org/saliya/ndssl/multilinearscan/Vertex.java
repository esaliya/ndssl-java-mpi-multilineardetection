package org.saliya.ndssl.multilinearscan;

import mpi.MPI;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class Vertex {
    Pattern pat = Pattern.compile(" ");
    int vertexLabel;
    int vertexId;
    double weight;
    TreeMap<Integer, Integer> outNeighborLabelToWorldRank;
    TreeMap<Integer, VertexBuffer> outrankToSendBuffer;
    List<RecvVertexBuffer> recvBuffers;

    Message message;
    List<Message> recvdMessages;

    public Vertex(){

    }
    public Vertex(int vertexId, String vertexLine){
        String[] splits = pat.split(vertexLine);
        this.vertexId = vertexId;
        vertexLabel = Integer.parseInt(splits[0]);
        weight = Double.parseDouble(splits[1]);
        outrankToSendBuffer = new TreeMap<>();
        outNeighborLabelToWorldRank = new TreeMap<>();
        for (int i = 2; i < splits.length; ++i){
            outNeighborLabelToWorldRank.put(Integer.parseInt(splits[i]), -1);
        }
        recvBuffers = new ArrayList<>();
        message = new Message(vertexLabel);
        recvdMessages = new ArrayList<>();
    }

    public boolean hasOutNeighbor(int outNeighborLabel){
        return outNeighborLabelToWorldRank.containsKey(outNeighborLabel);
    }

    public void compute(int superStep){
        /* compute logic */

        /* decide msg size */
        int msgSize = 1;

        message.msgSize = msgSize;
        message.data = vertexLabel;
    }

    public int prepareSend(int superStep, int shift){
        /* copy msg to outrankToSendBuffer */
        outrankToSendBuffer.entrySet().forEach(kv ->{
            VertexBuffer vertexBuffer = kv.getValue();
            int offset = shift + vertexBuffer.offsetFactor * message.msgSize;
            message.copyTo(vertexBuffer.buffer, offset);
        });
        return message.msgSize;
    }

    public void processRecvd(int superStep, int shift){

    }

}
