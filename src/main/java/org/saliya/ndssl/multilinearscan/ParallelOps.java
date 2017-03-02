package org.saliya.ndssl.multilinearscan;

import com.google.common.base.Strings;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class ParallelOps {
    public static String machineName;
    public static int nodeCount;
    public static int threadCount;
    public static String mmapScratchDir;

    public static Intracomm worldProcsComm;
    public static int worldProcRank;
    public static int worldProcsCount;

    public static IntBuffer oneIntBuffer;
    public static IntBuffer worldIntBuffer;
    public static IntBuffer vertexIntBuffer;

    // Will include same rank as well
    public static TreeMap<Integer, List<Integer>> recvfromRankToMsgCountAndforvertexLabels;


    public static void setupParallelism(String[] args) throws MPIException {
        MPI.Init(args);
        machineName = MPI.getProcessorName();
        worldProcsComm = MPI.COMM_WORLD; //initializing MPI world communicator
        worldProcRank = worldProcsComm.getRank();
        worldProcsCount = worldProcsComm.getSize();

        oneIntBuffer = MPI.newIntBuffer(1);
        worldIntBuffer = MPI.newIntBuffer(worldProcsCount);

    }

    public static void tearDownParallelism() throws MPIException {
        MPI.Finalize();

    }

    public static Vertex[] setParallelDecomposition(String file, int vertexCount) throws MPIException {
        /* Decompose input graph into processes */
        vertexIntBuffer = MPI.newIntBuffer(vertexCount);
        return simpleGraphPartition(file, vertexCount);
    }

    private static Vertex[] simpleGraphPartition(String file, int globalVertexCount) throws MPIException {
        /* Will assume vertex IDs are continuous and starts with zero
        * Then partition these vertices uniformly across all processes
        * Also, this assumes all vertices have out edges, otherwise we can't skip
        * lines like here.*/

        int q = globalVertexCount / worldProcsCount;
        int r = globalVertexCount % worldProcsCount;
        int myVertexCount = (worldProcRank < r) ? q+1: q;
        Vertex[] vertices = new Vertex[myVertexCount];
        int skipVertexCount = q*worldProcRank + (worldProcRank < r ? worldProcRank : r);
        int readCount = 0;
        int i = 0;

        // DEBUG
        //System.out.println("Rank: " + worldProcRank + " q: " + q + " r: " + r + " myVertexCount: " + myVertexCount);
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))){
            String line;
            while ((line = reader.readLine()) != null){
                if (Strings.isNullOrEmpty(line)) continue;
                if (readCount < skipVertexCount){
                    ++readCount;
                    continue;
                }
                vertices[i] = new Vertex(readCount, line);
                ++i;
                ++readCount;
                if (i == myVertexCount) break;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        findNeighbors(globalVertexCount, vertices);
        return vertices;
    }

    public static void findNeighbors(int globalVertexCount, Vertex[] vertices) throws MPIException {
        oneIntBuffer.put(0,vertices.length);
        worldProcsComm.allGather(oneIntBuffer, 1, MPI.INT, worldIntBuffer, 1, MPI.INT);
        int[] lengths = new int[worldProcsCount];
        worldIntBuffer.position(0);
        worldIntBuffer.get(lengths, 0, worldProcsCount);

        int[] displas = new int[worldProcsCount];
        System.arraycopy(lengths, 0, displas, 1, worldProcsCount - 1);
        Arrays.parallelPrefix(displas, (m, n) -> m + n);

        int displacement = displas[worldProcRank];
        for (int i = 0; i < vertices.length; ++i){
            vertexIntBuffer.put(i+displacement, vertices[i].vertexLabel);
        }
        worldProcsComm.allGatherv(vertexIntBuffer, lengths, displas, MPI.INT);


        /* Just keep in mind this table and the vertexIntBuffer can be really large
        * Think of optimizations if this becomes a bottleneck */
        Hashtable<Integer, Integer> vertexLabelToWorldRank = new Hashtable<>();
        {
            int rank = 0;
            do {
                int length = lengths[rank];
                displacement = displas[rank];
                for (int i = 0; i < length; ++i) {
                    vertexLabelToWorldRank.put(vertexIntBuffer.get(i + displacement), rank);
                }
                ++rank;
            } while (rank < worldProcsCount);
        }

        // Set where out-neighbors of vertices live
        for (Vertex v : vertices){
            TreeMap<Integer, Integer> outNeighborLabelToWorldRank = v.outNeighborLabelToWorldRank;
            for (int label : outNeighborLabelToWorldRank.keySet()){
                outNeighborLabelToWorldRank.put(label, vertexLabelToWorldRank.get(label));
            }
        }

        // ----------------
        TreeMap<Integer, List<Integer>> sendtoRankToMsgCountAndDestinedVertexLabels = new TreeMap<>();
        final int[] msgSize = {0};
        for (Vertex vertex : vertices){
            TreeMap<Integer, List<Integer>> inverseHT = new TreeMap<>();
            vertex.outNeighborLabelToWorldRank.entrySet().forEach(kv ->{
                int rank = kv.getValue();
                int destinedVertexLabel = kv.getKey();
                if (!inverseHT.containsKey(rank)){
                    inverseHT.put(rank, new ArrayList<>());
                }
                inverseHT.get(rank).add(destinedVertexLabel);
            });
            inverseHT.entrySet().forEach(kv -> {
                int rank = kv.getKey();
                List<Integer> list = kv.getValue();
                if (!sendtoRankToMsgCountAndDestinedVertexLabels.containsKey(rank)){
                    sendtoRankToMsgCountAndDestinedVertexLabels.put(rank, new ArrayList<>());
                    msgSize[0]+=2;
                }
                List<Integer> countAndDestinedVertexLabels = sendtoRankToMsgCountAndDestinedVertexLabels.get(rank);
                if (countAndDestinedVertexLabels.size() > 0) {
                    countAndDestinedVertexLabels.set(0, countAndDestinedVertexLabels.get(0) + 1);
                } else {
                    countAndDestinedVertexLabels.add(0, 1);
                }
                if (list.size() > 1){
                    countAndDestinedVertexLabels.add(-1*list.size());
                    countAndDestinedVertexLabels.addAll(list);
                    msgSize[0] += list.size()+1;
                } else {
                    countAndDestinedVertexLabels.add(list.get(0));
                    msgSize[0] += 1;
                }
            });
        }
        // DEBUG - print how many message counts and what are destined vertex labels (in order) for each rank from me
//        {
//            StringBuilder sb = new StringBuilder();
//            sb.append("\n--Rank: ").append(worldProcRank).append('\n');
//            for (Map.Entry<Integer, List<Integer>> kv : sendtoRankToMsgCountAndDestinedVertexLabels.entrySet()) {
//                List<Integer> list = kv.getValue();
//                sb.append(" sends ").append(list.get(0)).append(" msgs to rank ").append(kv.getKey()).append(" destined " +
//                        "to vertices ");
//                for (int i = 1; i < list.size(); ++i) {
//                    sb.append(list.get(i)).append(" ");
//                }
//                sb.append(" in order \n");
//            }
//            String msg = allReduce(sb.toString(), worldProcsComm);
//            if (worldProcRank == 0) {
//                System.out.println(msg);
//            }
//        }
        // ----------------

        // ################
        recvfromRankToMsgCountAndforvertexLabels = new TreeMap<>();
        oneIntBuffer.put(0, msgSize[0]);
        worldProcsComm.allReduce(oneIntBuffer, 1, MPI.INT, MPI.MAX);
        int maxBufferSize = oneIntBuffer.get(0)+1;// +1 to send msgSize
        IntBuffer buffer = MPI.newIntBuffer(maxBufferSize);
        for (int rank = 0; rank < worldProcsCount; ++rank){
            if (rank == worldProcRank){
                buffer.position(0);
                buffer.put(msgSize[0]);
                sendtoRankToMsgCountAndDestinedVertexLabels.entrySet().forEach(kv -> {
                    // numbers < -(globalVertexCount+1) will indicate ranks
                    buffer.put(-1*(kv.getKey()+globalVertexCount+1));
                    kv.getValue().forEach(buffer::put);
                });
            }
            worldProcsComm.bcast(buffer, maxBufferSize, MPI.INT, rank);

            buffer.position(0);
            int recvMsgSize = buffer.get();

            for (int i = 1; i <= recvMsgSize; ++i){
                int val = buffer.get(i);
                if (val < 0 && val < -1*(globalVertexCount)){
                    // It's the rank information
                    int senttoRank = (-1*val) - (globalVertexCount+1);
                    if (senttoRank == worldProcRank){
                        // It'll always be a unique rank, so no need to check if exists
                        List<Integer> list = new ArrayList<>();
                        recvfromRankToMsgCountAndforvertexLabels.put(rank, list);
                        for (int j = i+1; j <= recvMsgSize; ++j){
                            val = buffer.get(j);
                            if (val >= 0 || (val < 0 && val >= -1*globalVertexCount)){
                                list.add(val);
                            } else {
                                break;
                            }
                        }
                        break;
                    }
                }
            }
        }

        // DEBUG - print how many message counts and what are destined vertex labels (in order) for each rank from me
//        {
//            StringBuilder sb = new StringBuilder();
//            sb.append("\n##Rank: ").append(worldProcRank).append('\n');
//            for (Map.Entry<Integer, List<Integer>> kv : recvfromRankToMsgCountAndforvertexLabels.entrySet()) {
//                List<Integer> list = kv.getValue();
//                sb.append(" recvs ").append(list.get(0)).append(" msgs from rank ").append(kv.getKey()).append(" " +
//                        "for vertices ");
//                for (int i = 1; i < list.size(); ++i) {
//                    sb.append(list.get(i)).append(" ");
//                }
//                sb.append(" in order \n");
//            }
//            String msg = allReduce(sb.toString(), worldProcsComm);
//            if (worldProcRank == 0) {
//                System.out.println(msg);
//            }
//        }
        // ################



    }

    public static String allReduce(String value, Intracomm comm) throws MPIException {
        int [] lengths = new int[worldProcsCount];
        int length = value.length();
        lengths[worldProcRank] = length;
        comm.allGather(lengths, 1, MPI.INT);
        int [] displas = new int[worldProcsCount];
        displas[0] = 0;
        System.arraycopy(lengths, 0, displas, 1, worldProcsCount - 1);
        Arrays.parallelPrefix(displas, (m, n) -> m + n);
        int count = IntStream.of(lengths).sum(); // performs very similar to usual for loop, so no harm done
        char [] recv = new char[count];
        System.arraycopy(value.toCharArray(), 0,recv, displas[worldProcRank], length);
        comm.allGatherv(recv, lengths, displas, MPI.CHAR);
        return  new String(recv);
    }
}
