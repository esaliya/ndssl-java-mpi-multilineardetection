package org.saliya.ndssl.multilinearscan.mpi;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class Vertex {
    Pattern pat = Pattern.compile(" ");
    int vertexLabel;
    int vertexId;

    TreeMap<Integer, Integer> outNeighborLabelToWorldRank;
    TreeMap<Integer, VertexBuffer> outrankToSendBuffer;
    List<RecvVertexBuffer> recvBuffers;

    Message message;
    List<Message> recvdMessages;


    double weight;
    long uniqueRandomSeed;

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

    public void compute(int superStep){
        /* compute logic */

        /* decide msg size */
        int msgSize = 1;

        message.msgSize = msgSize;
        message.data = vertexLabel;

        if (superStep > 0){
            StringBuffer sb = new StringBuffer("\nVertex ");
            sb.append(vertexLabel).append(" recvd ");
            recvdMessages.forEach(m -> {
                sb.append(m.data).append(" ");
            });
            sb.append('\n');
            System.out.println(sb.toString());
        }
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
        for (int i = 0; i < recvBuffers.size(); ++i){
            RecvVertexBuffer recvVertexBuffer = recvBuffers.get(i);
            Message recvdMessage = recvdMessages.get(i);
            int recvdMsgSize = recvVertexBuffer
                    .getMessageSize
                            ();
            recvdMessage.loadFrom(recvVertexBuffer.buffer, shift+recvVertexBuffer.offsetFactor*recvdMsgSize, recvdMsgSize);
        }
    }

}
