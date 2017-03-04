package org.saliya.ndssl.multilinearscan;

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
    double weight;
    TreeMap<Integer, Integer> outNeighborLabelToWorldRank;
    TreeMap<Integer, VertexBuffer> outrankToSendBuffer;
    List<RecvVertexBuffer> recvBuffers;

    int msgSize = ParallelOps.MAX_MSG_SIZE;

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
    }

    public boolean hasOutNeighbor(int outNeighborLabel){
        return outNeighborLabelToWorldRank.containsKey(outNeighborLabel);
    }

    public void compute(){

    }
}
