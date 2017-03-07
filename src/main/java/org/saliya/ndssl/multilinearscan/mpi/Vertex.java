package org.saliya.ndssl.multilinearscan.mpi;

import org.saliya.ndssl.multilinearscan.GaloisField;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

    TreeMap<Integer, Integer> outNeighborLabelToWorldRank;
    TreeMap<Integer, VertexBuffer> outrankToSendBuffer;
    List<RecvVertexBuffer> recvBuffers;

    Message message;
    List<Message> recvdMessages;


    double weight;
    long uniqueRandomSeed;
    private int k;
    private int r;
    private GaloisField gf;
    private int[] cumulativeCompletionVariables;
    private int[][] totalSumTable;
    private int[][] optTable;
    private int[][] extTable;
    // Vertex unique random generator
    private Random random;

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
        if (superStep > 0){
            StringBuffer sb = new StringBuffer("\nVertex ");
            sb.append(vertexLabel).append(" recvd ");
            recvdMessages.forEach(m -> {
                m.toString(sb).append(" ");
            });
            sb.append('\n');
            System.out.println(sb.toString());
        }
    }

    public int prepareSend(int superStep, int shift){
        /* copy msg to outrankToSendBuffer */
        message.pack();
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
                    shift+recvVertexBuffer.offsetFactor*recvdMsgSize,
                    recvdMsgSize);
        }
    }

    public void init(int k, int r, GaloisField gf) {
        this.k = k;
        this.r = r;
        this.gf = gf;
        totalSumTable = new int[k+1][r+1];
        optTable = new int[k+1][r+1];
        extTable = new int[k+1][r+1];
        for (int i = 0; i <= k; i++) {
            for (int j = 0; j <= r; j++) {
                totalSumTable[i][j] = 0;
            }
        }
        cumulativeCompletionVariables = new int[k];
        
    }

    public void reset(int iter, int[] completionVariables, TreeMap<Integer, Integer> randomAssignments) {
        // the ith index of this array contains the polynomial
        // y_1 * y_2 * ... y_i
        // where y_0 is defined as the identity of the finite field, (i.e., 1)
        cumulativeCompletionVariables[0] = 1;
        for (int i = 1; i < k; i++) {
            int dotProduct = (completionVariables[i - 1] & iter); // dot product is bitwise and
            cumulativeCompletionVariables[i] = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 :
                    cumulativeCompletionVariables[i - 1];
        }

        // create the vertex unique random variable
        random = new Random(uniqueRandomSeed);
        // set arrays in vertex data
        IntStream.range(0,k+1).forEach(i ->
                IntStream.range(0, r+1).forEach(j -> {
            optTable[i][j] = 0;
            extTable[i][j] = 0;
        }));

        int nodeWeight = (int)weight;
        int dotProduct = (randomAssignments.get(vertexLabel) & iter); // dot product is bitwise and
        int eigenvalue = (Integer.bitCount(dotProduct) % 2 == 1) ? 0 : 1;
        for (int i = 0; i <= r; i++) {
            optTable[1][i] = extTable[1][i] = 0;
        }
        optTable[1][nodeWeight] = eigenvalue;
        extTable[1][nodeWeight] = eigenvalue * cumulativeCompletionVariables[k - 1];
        message.setDataAndMsgSize(optTable, (k+1)*(r+1));
    }
}
