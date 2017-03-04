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
    public static TreeMap<Integer, List<Integer>> sendtoRankToMsgCountAndDestinedVertexLabels;

    // Maximum message size sent by a vertex. To be set later correctly.
    public static final int MAX_MSG_SIZE = 500;
    public static TreeMap<Integer, IntBuffer> recvfromRankToRecvBuffer;
    public static TreeMap<Integer, IntBuffer> sendtoRankToSendBuffer;
    // to store msg count and msg size
    public static final int BUFFER_OFFSET = 2;


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
        TreeMap<Integer, Vertex> vertexLabelToVertex = new TreeMap<>();
        for (Vertex vertex : vertices){
            vertexLabelToVertex.put(vertex.vertexLabel, vertex);
        }
        oneIntBuffer.put(0,vertices.length);
        worldProcsComm.allGather(oneIntBuffer, 1, MPI.INT, worldIntBuffer, 1, MPI.INT);
        int[] lengths = new int[worldProcsCount];
        worldIntBuffer.position(0);
        worldIntBuffer.get(lengths, 0, worldProcsCount);

        int[] displas = new int[worldProcsCount];
        System.arraycopy(lengths, 0, displas, 1, worldProcsCount - 1);
        Arrays.parallelPrefix(displas, (m, n) -> m + n);

        // DEBUG - check lengths
        //if (worldProcRank == 1){
        //    for (int i = 0; i < worldProcsCount; ++i){
        //        System.out.println("Rank: " + i + " has " + worldIntBuffer.get(i) + " vertices");
        //    }
        //}


        // DEBUG - check displas
        //if (worldProcRank == 1) {
        //    System.out.println("Rank: " + worldProcRank + " displacements");
        //    for (int i = 0; i < worldProcsCount; ++i) {
        //        System.out.print(displas[i] + " ");
        //    }
        //}

        int displacement = displas[worldProcRank];
        for (int i = 0; i < vertices.length; ++i){
            vertexIntBuffer.put(i+displacement, vertices[i].vertexLabel);
        }
        worldProcsComm.allGatherv(vertexIntBuffer, lengths, displas, MPI.INT);


        // DEBUG - see what ranks have what vertices
//        if (worldProcRank == 0){
//            int rank = 0;
//            do{
//                System.out.print("\n\nRank: " + rank + " has ");
//                int length = lengths[rank];
//                displacement = displas[rank];
//                for (int i = 0; i < length; ++i){
//                    System.out.print(vertexIntBuffer.get(i+displacement) + " ");
//                }
//                ++rank;
//
//            } while (rank < worldProcsCount);
//        }


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
                Integer rank = vertexLabelToWorldRank.get(label);
                outNeighborLabelToWorldRank.put(label, rank);
                v.outrankToSendBuffer.put(rank, new VertexBuffer());
            }
        }


        // ++++++++++++++++
//        Hashtable<Integer, Hashtable<Integer, List<Integer>>> rankToVertexLabelToMyVerticesWithOutEdges = new Hashtable<>();
//        for (int rank = 0; rank < worldProcsCount; ++rank){
//            displacement = displas[rank];
//            // for each vertex in rank see if that vertex is an out-neighbor of any of my vertices
//            for (int i = 0; i < lengths[rank]; ++i){
//                int vertexLabel = vertexIntBuffer.get(i+displacement);
//                for (Vertex vertex : vertices){
//                    if (vertex.hasOutNeighbor(vertexLabel)){
//                        if (!rankToVertexLabelToMyVerticesWithOutEdges.containsKey(rank)){
//                            rankToVertexLabelToMyVerticesWithOutEdges.put(rank, new Hashtable<>());
//                        }
//                        Hashtable<Integer, List<Integer>> vertexLabelToMyVerticesWithOutEdges =
//                                rankToVertexLabelToMyVerticesWithOutEdges.get(rank);
//                        if (!vertexLabelToMyVerticesWithOutEdges.containsKey(vertexLabel)){
//                            vertexLabelToMyVerticesWithOutEdges.put(vertexLabel, new ArrayList<>());
//                        }
//                        List<Integer> list = vertexLabelToMyVerticesWithOutEdges.get(vertexLabel);
//                        list.add(vertex.vertexLabel);
//                    }
//                }
//            }
//        }
//
//        // DEBUG - print rankToVertexLabelToMyVerticesWithOutEdges
//        {
//            StringBuffer sb = new StringBuffer();
//            sb.append("Rank: ").append(worldProcRank).append('\n');
//            for (int rank = 0; rank < worldProcsCount; ++rank) {
//                Hashtable<Integer, List<Integer>> vertexLabelToMyVerticesWithOutEdges =
//                        rankToVertexLabelToMyVerticesWithOutEdges.get(rank);
//                if (vertexLabelToMyVerticesWithOutEdges == null) continue;
//
//                Set<Integer> keys = vertexLabelToMyVerticesWithOutEdges.keySet();
//                for (int key : keys) {
//                    sb.append("  has out edges to vertex ").append(key).append(" in rank ").append(rank).append(" from " +
//                            "vertices ");
//                    List<Integer> vs = vertexLabelToMyVerticesWithOutEdges.get(key);
//                    sb.append(Arrays.toString(vs.toArray())).append('\n');
//
//                }
//            }
//            String msg = allReduce(sb.toString(), worldProcsComm);
//            if (worldProcRank == 0) {
//                System.out.println(msg);
//            }
//        }
        // END ++++++++++++++++

        // ----------------
        sendtoRankToMsgCountAndDestinedVertexLabels = new TreeMap<>();
        final int[] msgSize = {0};
        // The vertex order is important as it's implicitly assumed to reduce communication cost
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
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n--Rank: ").append(worldProcRank).append('\n');
            for (Map.Entry<Integer, List<Integer>> kv : sendtoRankToMsgCountAndDestinedVertexLabels.entrySet()) {
                List<Integer> list = kv.getValue();
                sb.append(" sends ").append(list.get(0)).append(" msgs to rank ").append(kv.getKey()).append(" destined " +
                        "to vertices ");
                for (int i = 1; i < list.size(); ++i) {
                    sb.append(list.get(i)).append(" ");
                }
                sb.append(" in order \n");
            }
            String msg = allReduce(sb.toString(), worldProcsComm);
            if (worldProcRank == 0) {
                System.out.println(msg);
            }
        }
        // END ----------------

        // ################
        recvfromRankToMsgCountAndforvertexLabels = new TreeMap<>();
        oneIntBuffer.put(0, msgSize[0]);
        worldProcsComm.allReduce(oneIntBuffer, 1, MPI.INT, MPI.MAX);
        int maxBufferSize = oneIntBuffer.get(0)+1;// +1 to send msgSize
        {
            IntBuffer buffer = MPI.newIntBuffer(maxBufferSize);
            for (int rank = 0; rank < worldProcsCount; ++rank) {
                if (rank == worldProcRank) {
                    buffer.position(0);
                    buffer.put(msgSize[0]);
                    sendtoRankToMsgCountAndDestinedVertexLabels.entrySet().forEach(kv -> {
                        // numbers < -(globalVertexCount+1) will indicate ranks
                        buffer.put(-1 * (kv.getKey() + globalVertexCount + 1));
                        kv.getValue().forEach(buffer::put);
                    });
                }
                worldProcsComm.bcast(buffer, maxBufferSize, MPI.INT, rank);

                buffer.position(0);
                int recvMsgSize = buffer.get();

                for (int i = 1; i <= recvMsgSize; ++i) {
                    int val = buffer.get(i);
                    if (val < 0 && val < -1 * (globalVertexCount)) {
                        // It's the rank information
                        int senttoRank = (-1 * val) - (globalVertexCount + 1);
                        if (senttoRank == worldProcRank) {
                            // It'll always be a unique rank, so no need to check if exists
                            List<Integer> list = new ArrayList<>();
                            recvfromRankToMsgCountAndforvertexLabels.put(rank, list);
                            for (int j = i + 1; j <= recvMsgSize; ++j) {
                                val = buffer.get(j);
                                if (val >= 0 || (val < 0 && val >= -1 * globalVertexCount)) {
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
        }

        // DEBUG - print how many message counts and what are destined vertex labels (in order) for each rank from me
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n##Rank: ").append(worldProcRank).append('\n');
            for (Map.Entry<Integer, List<Integer>> kv : recvfromRankToMsgCountAndforvertexLabels.entrySet()) {
                List<Integer> list = kv.getValue();
                sb.append(" recvs ").append(list.get(0)).append(" msgs from rank ").append(kv.getKey()).append(" " +
                        "for vertices ");
                for (int i = 1; i < list.size(); ++i) {
                    sb.append(list.get(i)).append(" ");
                }
                sb.append(" in order \n");
            }
            String msg = allReduce(sb.toString(), worldProcsComm);
            if (worldProcRank == 0) {
                System.out.println(msg);
            }
        }
        // END ################

        // ~~~~~~~~~~~~~~~~
        recvfromRankToRecvBuffer = new TreeMap<>();
        recvfromRankToMsgCountAndforvertexLabels.entrySet().forEach(kv -> {
            int recvfromRank = kv.getKey();
            List<Integer> list = kv.getValue();
            int msgCount = list.get(0);
            IntBuffer b = MPI.newIntBuffer(BUFFER_OFFSET + msgCount * MAX_MSG_SIZE);
            recvfromRankToRecvBuffer.put(recvfromRank, b);
            int currentMsg = 0;
            for (int i = 1; i < list.size(); ){
                int val = list.get(i);
                if (val >= 0){
                    Vertex vertex = vertexLabelToVertex.get(val);
                    vertex.recvBuffers.add(new RecvVertexBuffer(currentMsg, b, recvfromRank));
                    currentMsg++;
                    ++i;
                } else if (val < 0) {
                    int intendedVertexCount = -1*val;
                    for (int j = i+1; j <= intendedVertexCount+i; ++j){
                        val = list.get(j);
                        Vertex vertex = vertexLabelToVertex.get(val);
                        vertex.recvBuffers.add(new RecvVertexBuffer(currentMsg, b, recvfromRank));
                    }
                    i+=intendedVertexCount+1;
                    currentMsg++;
                }
            }
        });

        // DEBUG
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n##Rank: ").append(worldProcRank).append('\n');
            for (Vertex vertex : vertices){
                sb.append("  vertexLabel ").append(vertex.vertexLabel).append(" recvs \n");
                vertex.recvBuffers.forEach(recvVertexBuffer -> {
                    sb.append("    from rank ").append(recvVertexBuffer.recvfromRank).append(" offsetFactor ").append
                            (recvVertexBuffer.getOffsetFactor()).append('\n');
                });
            }
            String msg = allReduce(sb.toString(), worldProcsComm);
            if (worldProcRank == 0) {
                System.out.println(msg);
            }
        }
        // END ~~~~~~~~~~~~~~~~

        sendtoRankToSendBuffer = new TreeMap<>();
        sendtoRankToMsgCountAndDestinedVertexLabels.entrySet().forEach(kv -> {
            int sendtoRank = kv.getKey();
            int msgCount = kv.getValue().get(0);
            // +2 to store msgCount and msgSize
            IntBuffer b = MPI.newIntBuffer(BUFFER_OFFSET +msgCount*MAX_MSG_SIZE);
            b.put(0, msgCount);
            sendtoRankToSendBuffer.put(sendtoRank, b);
        });

        int offsetFactor = 0;
        for (Vertex vertex : vertices){
            int finalOffsetFactor = offsetFactor;
            vertex.outrankToSendBuffer.keySet().forEach(k -> {
                VertexBuffer vertexSendBuffer = vertex.outrankToSendBuffer.get(k);
                vertexSendBuffer.setOffsetFactor(finalOffsetFactor);
                vertexSendBuffer.setBuffer(sendtoRankToSendBuffer.get(k));
            });
            ++offsetFactor;
        }



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
