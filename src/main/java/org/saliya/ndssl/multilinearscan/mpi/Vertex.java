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
    private short[][] optTable;
    private short[][] extTable;
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

    public void compute(int superStep, int iter, int[] completionVariables, TreeMap<Integer, Integer>
            randomAssignments){

        // In the original code I started from 2 and went till k (including).
        // we start ss from zero, which is for initial msg sending
        // then real work begins with ss=1, which should give I=2, hence I=ss+1
        int I = superStep+1;

        /* compute logic */
        if (superStep == 0){
            reset(iter, completionVariables, randomAssignments);
        } else if (superStep > 0){
            // DEBUG - see received messages
            /*StringBuffer sb = new StringBuffer("\nVertex ");
            sb.append(vertexLabel).append(" recvd ");
            recvdMessages.forEach(m -> {
                m.toString(sb).append(" ");
            });
            sb.append('\n');
            System.out.println(sb.toString());*/
            
            // Business logic
            int fieldSize = gf.getFieldSize();
            // for every quota l from 0 to r
            for (int l = 0; l <= r; l++) {
                // initialize the polynomial P_{i,u,l}
                int polynomial = 0;
                // recursive step:
                // iterate through all the pairs of polynomials whose sizes add up to i
                for (int iPrime = 1; iPrime < I; iPrime++) {
                    for (Message msg : recvdMessages) {
//                        short[][] neighborOptTable = msg.getData();
                        if (l == 0) {
                            int weight = random.nextInt(fieldSize);
//                            int product = gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][0]);
                            int product = gf.multiply(optTable[iPrime][0], msg.get(I - iPrime,0));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);
                        } else if (l == 1) {
                            int weight = random.nextInt(fieldSize);
//                            int product = gf.multiply(optTable[iPrime][1], neighborOptTable[I - iPrime][0]);
                            int product = gf.multiply(optTable[iPrime][1], msg.get(I - iPrime,0));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
//                            product = gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][1]);
                            product = gf.multiply(optTable[iPrime][0], msg.get(I - iPrime,1));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);
                        } else {
                            int weight = random.nextInt(fieldSize);
//                            int product = gf.multiply(optTable[iPrime][l - 1], neighborOptTable[I - iPrime][l -
                            int product = gf.multiply(optTable[iPrime][l - 1], msg.get(I - iPrime,l -
                                    1));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
//                            product = gf.multiply(optTable[iPrime][l], neighborOptTable[I - iPrime][0]);
                            product = gf.multiply(optTable[iPrime][l], msg.get(I - iPrime,0));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);

                            weight = random.nextInt(fieldSize);
//                            product = gf.multiply(optTable[iPrime][0], neighborOptTable[I - iPrime][l]);
                            product = gf.multiply(optTable[iPrime][0], msg.get(I - iPrime,l));
                            product = gf.multiply(weight, product);
                            polynomial = gf.add(polynomial, product);
                        }
                    }
                }
                optTable[I][l] = (short)polynomial;
                if (cumulativeCompletionVariables[k - I] != 0) {
                    extTable[I][l] = optTable[I][l];
                }
            }
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
        optTable = new short[k+1][r+1];
        extTable = new short[k+1][r+1];
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
        optTable[1][nodeWeight] = (short)eigenvalue;
        extTable[1][nodeWeight] = (short)(eigenvalue * cumulativeCompletionVariables[k - 1]);
        message.setDataAndMsgSize(optTable, (k+1)*(r+1));
    }

    public void finalizeIteration() {
        // aggregate to master
        // hmm, but we don't need to aggregate, just add to totalSumTable of the vertex
        for (int kPrime = 0; kPrime <= k; kPrime++) {
            for (int rPrime = 0; rPrime <= r; rPrime++) {
                totalSumTable[kPrime][rPrime] = gf.add(totalSumTable[kPrime][rPrime],
                        extTable[kPrime][rPrime]);
            }
        }
    }

    public double finalizeIterations(double alphaMax, int roundingFactor) {
        // Now, we can change the totalSumTable to the decisionTable
        for (int kPrime = 0; kPrime <= k; kPrime++) {
            for (int rPrime = 0; rPrime <= r; rPrime++) {
                totalSumTable[kPrime][rPrime] = (totalSumTable[kPrime][rPrime] > 0) ? 1 : -1;
            }
        }
        return getScoreFromTablePower(totalSumTable, alphaMax, roundingFactor);
    }

    // This is node local best score, not the global best
    // to get the global best we have to find the max of these local best scores,
    // which we'll do in the master using aggregation
    private double getScoreFromTablePower(int[][] existenceForNode, double alpha, int roundingFactor) {
        double nodeLocalBestScore = 0;
        for (int kPrime = 1; kPrime < existenceForNode.length; kPrime++) {
            for (int rPrime = 0; rPrime < existenceForNode[0].length; rPrime++) {
                if (existenceForNode[kPrime][rPrime] == 1) {
                    //System.out.println("Found subgraph with size " + kPrime + " and score " + rPrime);
                    // size cannot be smaller than weight
                    int unroundedPrize = (int) Math.pow(roundingFactor, rPrime - 1);
                    // adjust for the fact that this is the refined graph
                    int unroundedSize = Math.max(kPrime, unroundedPrize);
                    double score = Utils.BJ(alpha, unroundedPrize, unroundedSize);
                    //System.out.println("Score is " + score);
                    nodeLocalBestScore = Math.max(nodeLocalBestScore, score);
                }
            }
        }
        return nodeLocalBestScore;
    }
}
