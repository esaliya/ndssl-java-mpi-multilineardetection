package org.saliya.ndssl.multilinearscan;

import java.util.Hashtable;
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

    public Vertex(){

    }
    public Vertex(int vertexId, String vertexLine){
        String[] splits = pat.split(vertexLine);
        this.vertexId = vertexId;
        vertexLabel = Integer.parseInt(splits[0]);
        weight = Double.parseDouble(splits[1]);
        outNeighborLabelToWorldRank = new TreeMap<>();
        for (int i = 2; i < splits.length; ++i){
            outNeighborLabelToWorldRank.put(Integer.parseInt(splits[i]), -1);
        }

    }

    public boolean hasOutNeighbor(int outNeighborLabel){
        return outNeighborLabelToWorldRank.containsKey(outNeighborLabel);
    }
}
