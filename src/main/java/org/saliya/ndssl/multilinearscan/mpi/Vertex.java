package org.saliya.ndssl.multilinearscan.mpi;

import org.saliya.ndssl.Utils;
import org.saliya.ndssl.multilinearscan.GaloisField;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class Vertex {
    Pattern pat = Pattern.compile(" ");
    int vertexLabel;
    int vertexId;
    int vertexLevel;
    char vertexOp;

    TreeMap<Integer, Integer> outNeighborLabelToWorldRank;
    TreeMap<Integer, VertexBuffer> outrankToSendBuffer;
    List<RecvVertexBuffer> recvBuffers;

    Message message;
    List<Message> recvdMessages;


    private GaloisField gf;

    // Vertex unique random generator
    private Random random;

    public Vertex(){

    }

    @Override
    public String toString() {
        return String.valueOf(vertexLabel);
    }

    public Vertex(int vertexId, String vertexLine){
        String[] splits = pat.split(vertexLine);
        this.vertexId = vertexId;
        vertexLabel = Integer.parseInt(splits[0]);
        vertexLevel = Integer.parseInt(splits[1]) - 1;
        vertexOp = splits[2].charAt(0);

        outrankToSendBuffer = new TreeMap<>();
        outNeighborLabelToWorldRank = new TreeMap<>();
        for (int i = 3; i < splits.length; ++i){
            outNeighborLabelToWorldRank.put(Integer.parseInt(splits[i]), -1);
        }
        recvBuffers = new ArrayList<>();
        // TODO - set a random 0 and 1 to leaf nodes and -1 for everything else, just to test
        message = new Message((short)(vertexLevel == 0 ? (Math.random() > 0.5 ? 1 : 0) : -1));
        recvdMessages = new ArrayList<>();
    }

    public void compute(int superStep){

    }

    public int prepareSend(int superStep, int shift){
        /* copy msg to outrankToSendBuffer */
        outrankToSendBuffer.entrySet().forEach(kv ->{
            VertexBuffer vertexBuffer = kv.getValue();
            int offset = shift + vertexBuffer.offsetFactor * message.getMsgSize();
            message.copyTo(vertexBuffer.buffer, offset);
        });
        return message.getMsgSize();
    }

    public void processRecvd(int superStep, int shift){
        for (int i = 0; i < recvBuffers.size(); ++i){
            RecvVertexBuffer recvVertexBuffer = recvBuffers.get(i);
            Message recvdMessage = recvdMessages.get(i);
            int recvdMsgSize = recvVertexBuffer.getMessageSize();
            recvdMessage.loadFrom(
                    recvVertexBuffer.buffer,
                    shift+recvVertexBuffer.offsetFactor*recvdMsgSize
            );
        }
    }
}
