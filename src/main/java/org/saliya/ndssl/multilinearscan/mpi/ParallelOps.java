package org.saliya.ndssl.multilinearscan.mpi;

import com.google.common.base.Strings;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
    public static LongBuffer oneLongBuffer;
    public static DoubleBuffer oneDoubleBuffer;
    public static ByteBuffer oneByteBuffer;
    public static IntBuffer worldIntBuffer;
    public static IntBuffer vertexIntBuffer;
//    public static LongBuffer vertexLongBuffer;
    public static DoubleBuffer vertexDoubleBuffer;

    // Will include same rank as well
    public static TreeMap<Integer, List<Integer>> recvfromRankToMsgCountAndforvertexLabels;
    public static TreeMap<Integer, List<Integer>> sendtoRankToMsgCountAndDestinedVertexLabels;

    // Maximum message size sent by a vertex. To be set later correctly.
    public static int MAX_MSG_SIZE = 500;
    public static TreeMap<Integer, ShortBuffer> recvfromRankToRecvBuffer;
    public static TreeMap<Integer, ShortBuffer> sendtoRankToSendBuffer;
    // to store msg count and msg size -- note msg count is stored as two shorts
    public static final int BUFFER_OFFSET = 3;
    public static final int MSG_COUNT_OFFSET = 0;
    public static final int MSG_SIZE_OFFSET = 2;

    public static int msgSizeToReceive;

    public static TreeMap<Integer, Request> requests;

    private static boolean debug = false;
    private static boolean debug2 = false;
    private static boolean debug3 = true;
    public static int[] localVertexCounts;
    public static int[] localVertexDisplas;

    public static Hashtable<Integer, Integer> vertexLabelToWorldRank;
    public static ByteBuffer converterByteBuffer;
    public static int[] threadIdToVertexCount;
    public static int[] threadIdToVertexOffset;
    public static ThreadCommunicator threadComm;

    public static int recvRequestOffset;
    public static Request[] sendRecvRequests;
    // The number of parallel copies we are running, so we can cutdown the number of
    // iterations to do in the program
    public static int parallelInstance;
    public static int parallelInstanceCount;


    public static void setupParallelism(String[] args) throws MPIException {
        MPI.Init(args);
        machineName = MPI.getProcessorName();
        worldProcsComm = MPI.COMM_WORLD; //initializing MPI world communicator
        worldProcRank = worldProcsComm.getRank();
        worldProcsCount = worldProcsComm.getSize();

        threadComm = new ThreadCommunicator(threadCount);

        oneIntBuffer = MPI.newIntBuffer(1);
        oneByteBuffer = MPI.newByteBuffer(1);
        oneLongBuffer = MPI.newLongBuffer(1);
        oneDoubleBuffer = MPI.newDoubleBuffer(1);
        worldIntBuffer = MPI.newIntBuffer(worldProcsCount);
        converterByteBuffer = MPI.newByteBuffer(Integer.BYTES);

    }

    public static void tearDownParallelism() throws MPIException {
        MPI.Finalize();

    }

    public static Vertex[] setParallelDecomposition(String file, int vertexCount, String partitionFile) throws
            MPIException {
        long t = System.currentTimeMillis();
        /* Decompose input graph into processes */
        vertexIntBuffer = MPI.newIntBuffer(vertexCount);
//        vertexLongBuffer = MPI.newLongBuffer(vertexCount);
        vertexDoubleBuffer = MPI.newDoubleBuffer(vertexCount);

        if (debug3 && worldProcRank == 0){
            System.out.println("Rank: 0 allocate vertex buffers: " + (System.currentTimeMillis() - t) + " ms");
        }

        String partitionMethod = "SimpleLoadBalance";
        Vertex[] vertices;
        if (Strings.isNullOrEmpty(partitionFile)){
            vertices = simpleGraphPartitionForBinaryFiles(file, vertexCount);
        } else {
            File f = new File(partitionFile);
            if (f.exists()) {
                partitionMethod = "Metis";
                vertices = metisGraphPartition(file, partitionFile, vertexCount);
            } else {
                vertices = simpleGraphPartitionForBinaryFiles(file, vertexCount);
            }
        }

        if(worldProcRank == 0){
            System.out.println("  Partitioning Method: " + partitionMethod);
        }
        decomposeAmongThreads(vertices);
        return vertices;
    }

    private static Vertex[] metisGraphPartition(String graphFile, String partitionFile, int globalVertexCount) throws MPIException {
        try(BufferedReader graphReader = Files.newBufferedReader(Paths.get(graphFile));
            BufferedReader partitionReader = Files.newBufferedReader(Paths.get(partitionFile))) {
            TreeSet<Integer> myNodeIds = new TreeSet<>();
            int nodeId = 0;
            String line;
            while (!Strings.isNullOrEmpty(line = partitionReader.readLine())){
                int partitionId = Integer.parseInt(line);
                if ((partitionId/threadCount) == worldProcRank){
                    myNodeIds.add(nodeId);
                }
                ++nodeId;
            }

            Vertex[] vertices = new Vertex[myNodeIds.size()];

            nodeId = 0;
            int readNodes = 0;
            while ((line = graphReader.readLine()) != null){
                if (Strings.isNullOrEmpty(line)) continue;
                if (myNodeIds.contains(nodeId)){
                    vertices[readNodes] = new Vertex(nodeId, line);
                    ++readNodes;
                }
                ++nodeId;
            }

            findNeighbors(globalVertexCount, vertices);
            return vertices;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void decomposeAmongThreads(Vertex[] vertices) {
        int length = vertices.length;
        int p = length / threadCount;
        int q = length % threadCount;
        threadIdToVertexOffset = new int[threadCount];
        threadIdToVertexCount = new int[threadCount];
        for (int i = 0; i < threadCount; ++i){
            threadIdToVertexCount[i] = (i < q) ? (p+1) : p;
        }
        threadIdToVertexOffset[0] = 0;
        System.arraycopy(threadIdToVertexCount, 0, threadIdToVertexOffset, 1, threadCount - 1);
        Arrays.parallelPrefix(threadIdToVertexOffset, (m, n) -> m + n);
    }

    public static void main(String[] args) throws MPIException {
        String file = args[0];
        String name = com.google.common.io.Files.getNameWithoutExtension(file);
        String dir = new File(file).getParent();
        Path outputFile = Paths.get(dir, name+".bin");
        int globalVertexCount = Integer.parseInt(args[1]);

//        writeBinary(file, outputFile, globalVertexCount);
//        readBinary(outputFile, globalVertexCount);

        worldProcsCount = 86;
        worldProcRank = 27;
        simpleGraphPartitionForBinaryFiles(outputFile.toString(), globalVertexCount);

    }

    private static void readBinary(Path outputFile, int globalVertexCount) {
        try(BufferedInputStream bis = new BufferedInputStream(
                Files.newInputStream(outputFile, StandardOpenOption.READ))){
            DataInputStream dis = new DataInputStream(bis);
            for (int i = 0; i < globalVertexCount; ++i){
                System.out.println(dis.readInt() + " " + dis.readInt());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeBinary(String file, Path outputFile, int globalVertexCount) {
        int readCount = 0;
        long t = System.currentTimeMillis();
        int[][] numNeighbors = new int[globalVertexCount][2];
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(file));
             BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(outputFile,
                        StandardOpenOption.CREATE))){

            DataOutputStream dos = new DataOutputStream(bos);
            for (int i = 0; i < globalVertexCount; ++i){
                dos.writeInt(-1);
                dos.writeInt(-1);
            }

            String line;
            DecimalFormat df = new DecimalFormat("#.##");
            df.setRoundingMode(RoundingMode.FLOOR);
            Pattern pat = Pattern.compile(" ");
            String[] splits;

            while ((line = reader.readLine()) != null){
                if (Strings.isNullOrEmpty(line)) continue;

                if (readCount%5000 == 0){
                    System.out.println("Read: " + readCount + " " + df.format
                            (readCount*100.0/globalVertexCount) +
                            "% " +
                            "lines in" +
                            " " + (System
                            .currentTimeMillis()
                            - t)
                            + " ms");
                }

                splits = pat.split(line);
                numNeighbors[readCount][0] = Integer.parseInt(splits[0]);
                numNeighbors[readCount][1] = splits.length - 1;
                Stream.of(splits).forEach(i -> {
                    try {
                        dos.writeInt(Integer.parseInt(i));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                ++readCount;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try(BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(outputFile,StandardOpenOption
                .WRITE))){
            DataOutputStream dos = new DataOutputStream(bos);
            for (int i = 0; i < globalVertexCount; ++i){
                dos.writeInt(numNeighbors[i][0]);
                dos.writeInt(numNeighbors[i][1]);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Vertex[] simpleGraphPartitionForBinaryFiles(String file, int globalVertexCount) throws MPIException{
        /* Will assume vertex IDs are continuous and starts with zero
        * Then partition these vertices uniformly across all processes
        * Also, this assumes all vertices have out edges, otherwise we can't skip
        * lines like here.*/

        int q = globalVertexCount / worldProcsCount;
        int r = globalVertexCount % worldProcsCount;
        int myVertexCount = (worldProcRank < r) ? q+1: q;
        Vertex[] vertices = new Vertex[myVertexCount];
        int skipVertexCount = q*worldProcRank + (worldProcRank < r ? worldProcRank : r);

        long t = System.currentTimeMillis();
        try (FileChannel fc = (FileChannel) Files.newByteChannel(Paths.get(
                file), StandardOpenOption.READ)) {
            MappedByteBuffer headerMap;
            MappedByteBuffer dataMap;

            long headerExtent = ((long) globalVertexCount) * 2 * Integer.BYTES;
            headerMap = fc.map(FileChannel.MapMode.READ_ONLY, 0, headerExtent);
            long dataOffset = 0;
            long dataExtent = 0;
            for (int i = 0; i < globalVertexCount; ++i){
                if (skipVertexCount == i){
                    break;
                }
                headerMap.getInt();//skip-node id
                // skip-node's weight+neighbors
                // +1 because we store node id as well
                dataOffset += (long)(headerMap.getInt()+1);
            }
            dataOffset *= Integer.BYTES;

            int[] vertexNeighborLength = new int[myVertexCount];
            int[] outNeighbors = new int[globalVertexCount];
            long runningExtent;
            long readExtent = 0L;
            int readVertex = -1;
            for (int i = 0; i < myVertexCount; ++i){
                headerMap.getInt();// my ith vertex's node id
                // my ith vertex's weight+neighbors
                //+ +1 because we store node id as well
                int len = headerMap.getInt();
                vertexNeighborLength[i] = len-1;

                runningExtent = dataExtent + (long)(len +1);
                if (runningExtent*Integer.BYTES <= Integer.MAX_VALUE){
                    dataExtent = runningExtent;
                } else {
                    dataExtent = readVertices(vertices, skipVertexCount, fc, headerExtent, dataOffset,
                            dataExtent, vertexNeighborLength, outNeighbors, readExtent, readVertex, i);
                    readExtent += dataExtent;
                    dataExtent = (long)(len +1);
                    readVertex = (i-1);
                }
            }
            readVertices(vertices, skipVertexCount, fc, headerExtent, dataOffset,
                    dataExtent, vertexNeighborLength, outNeighbors, readExtent, readVertex, myVertexCount);
        } catch (IOException e) {
            System.out.println("***ERROR: Rank: " + worldProcRank + " of " + worldProcsCount + " " + e.getMessage());
        }
        if (debug3){
            System.out.println("Rank: " + worldProcRank + " readgraph: "+ (System.currentTimeMillis() - t) + " ms");
        }

        t = System.currentTimeMillis();
        findNeighbors(globalVertexCount, vertices);
        if (debug3 && worldProcRank == 0){
            System.out.println("Rank: 0 findNbrs: "+ (System.currentTimeMillis() - t) + " ms");
        }
        return vertices;

    }

    private static long readVertices(Vertex[] vertices, int skipVertexCount, FileChannel fc, long headerExtent, long dataOffset, long dataExtent, int[] vertexNeighborLength, int[] outNeighbors, long readExtent, int readVertex, int i) throws IOException {
        MappedByteBuffer dataMap;
        dataExtent *= Integer.BYTES;
        dataMap = fc.map(FileChannel.MapMode.READ_ONLY,
                dataOffset+headerExtent+readExtent, dataExtent);
        for (int j = readVertex+1; j < i; ++j){
            int vertexLabel = dataMap.getInt();
            double vertexWeight = dataMap.getInt();
            for (int k = 0; k < vertexNeighborLength[j]; ++k){
                outNeighbors[k] = dataMap.getInt();
            }
            vertices[j] = new Vertex(j+skipVertexCount, vertexLabel, vertexWeight, outNeighbors,
                    vertexNeighborLength[j]);
        }
        return dataExtent;
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
        long t = System.currentTimeMillis();
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
        if (debug3 && worldProcRank == 0){
            System.out.println("Rank: 0 readgraph: "+ (System.currentTimeMillis() - t) + " ms");
        }

        t = System.currentTimeMillis();
        findNeighbors(globalVertexCount, vertices);
        if (debug3 && worldProcRank == 0){
            System.out.println("Rank: 0 findNbrs: "+ (System.currentTimeMillis() - t) + " ms");
        }
        return vertices;
    }

    public static void findNeighbors(int globalVertexCount, Vertex[] vertices) throws MPIException {
        TreeMap<Integer, Vertex> vertexLabelToVertex = new TreeMap<>();
        for (Vertex vertex : vertices){
            vertexLabelToVertex.put(vertex.vertexLabel, vertex);
        }
        oneIntBuffer.put(0,vertices.length);
        worldProcsComm.allGather(oneIntBuffer, 1, MPI.INT, worldIntBuffer, 1, MPI.INT);
        localVertexCounts = new int[worldProcsCount];
        worldIntBuffer.position(0);
        worldIntBuffer.get(localVertexCounts, 0, worldProcsCount);

        localVertexDisplas = new int[worldProcsCount];
        System.arraycopy(localVertexCounts, 0, localVertexDisplas, 1, worldProcsCount - 1);
        Arrays.parallelPrefix(localVertexDisplas, (m, n) -> m + n);

        // DEBUG - check localVertexCounts
        //if (worldProcRank == 1){
        //    for (int i = 0; i < worldProcsCount; ++i){
        //        System.out.println("Rank: " + i + " has " + worldIntBuffer.get(i) + " vertices");
        //    }
        //}


        // DEBUG - check localVertexDisplas
        //if (worldProcRank == 1) {
        //    System.out.println("Rank: " + worldProcRank + " displacements");
        //    for (int i = 0; i < worldProcsCount; ++i) {
        //        System.out.print(localVertexDisplas[i] + " ");
        //    }
        //}

        int displacement = localVertexDisplas[worldProcRank];
        for (int i = 0; i < vertices.length; ++i){
            vertexIntBuffer.put(i+displacement, vertices[i].vertexLabel);
        }
        worldProcsComm.allGatherv(vertexIntBuffer, localVertexCounts, localVertexDisplas, MPI.INT);


        // DEBUG - see what ranks have what vertices
        if (debug && worldProcRank == 0){
            int rank = 0;
            do{
                System.out.print("\n\nRank: " + rank + " has ");
                int length = localVertexCounts[rank];
                displacement = localVertexDisplas[rank];
                for (int i = 0; i < length; ++i){
                    System.out.print(vertexIntBuffer.get(i+displacement) + " ");
                }
                ++rank;

            } while (rank < worldProcsCount);
        }


        /* Just keep in mind this table and the vertexIntBuffer can be really large
        * Think of optimizations if this becomes a bottleneck */
        vertexLabelToWorldRank = new Hashtable<>();
        {
            int rank = 0;
            do {
                int length = localVertexCounts[rank];
                displacement = localVertexDisplas[rank];
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
        if (debug){
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
        if (debug){
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
        requests = new TreeMap<>();
        recvfromRankToRecvBuffer = new TreeMap<>();
        recvfromRankToMsgCountAndforvertexLabels.entrySet().forEach(kv -> {
            int recvfromRank = kv.getKey();
            List<Integer> list = kv.getValue();
            int msgCount = list.get(0);
            ShortBuffer b = MPI.newShortBuffer(BUFFER_OFFSET + msgCount * MAX_MSG_SIZE);
            recvfromRankToRecvBuffer.put(recvfromRank, b);
            int currentMsg = 0;
            for (int i = 1; i < list.size(); ){
                int val = list.get(i);
                if (val >= 0){
                    Vertex vertex = vertexLabelToVertex.get(val);
                    vertex.recvBuffers.add(new RecvVertexBuffer(currentMsg, b, recvfromRank, MSG_SIZE_OFFSET));
                    vertex.recvdMessages.add(new Message());
                    currentMsg++;
                    ++i;
                } else if (val < 0) {
                    int intendedVertexCount = -1*val;
                    for (int j = i+1; j <= intendedVertexCount+i; ++j){
                        val = list.get(j);
                        Vertex vertex = vertexLabelToVertex.get(val);
                        vertex.recvBuffers.add(new RecvVertexBuffer(currentMsg, b, recvfromRank, MSG_SIZE_OFFSET));
                        vertex.recvdMessages.add(new Message());
                    }
                    i+=intendedVertexCount+1;
                    currentMsg++;
                }
            }
        });

        // DEBUG
        if (debug) {
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
            ShortBuffer b = MPI.newShortBuffer(BUFFER_OFFSET +msgCount*MAX_MSG_SIZE);
            converterByteBuffer.putInt(0, msgCount);
            b.put(MSG_COUNT_OFFSET, converterByteBuffer.getShort(0));
            b.put(MSG_COUNT_OFFSET+1, converterByteBuffer.getShort(Byte.BYTES));
            sendtoRankToSendBuffer.put(sendtoRank, b);
        });


        TreeMap<Integer, Integer> outrankToOffsetFactor = new TreeMap<>();
        for (Vertex vertex : vertices){
            Set<Integer> vertexOutRanks = vertex.outrankToSendBuffer.keySet();
            vertexOutRanks.forEach(outRank -> {
                if (!outrankToOffsetFactor.containsKey(outRank)) {
                    outrankToOffsetFactor.put(outRank, 0);
                } else {
                    outrankToOffsetFactor.put(outRank, outrankToOffsetFactor.get(outRank)+1);
                }
                VertexBuffer vertexSendBuffer = vertex.outrankToSendBuffer.get(outRank);
                vertexSendBuffer.setOffsetFactor(outrankToOffsetFactor.get(outRank));
                vertexSendBuffer.setBuffer(sendtoRankToSendBuffer.get(outRank));
            });

        }

        if (debug2){
            StringBuilder sb = new StringBuilder();
            sb.append("--Rank: ").append(worldProcRank).append(" ");
            int recvfromRankCount = recvfromRankToMsgCountAndforvertexLabels.size();
            int recvMsgCount = 0;
            for (List<Integer> l : recvfromRankToMsgCountAndforvertexLabels.values()){
                recvMsgCount += l.get(0);
            }
            int sendtoRankCount = sendtoRankToMsgCountAndDestinedVertexLabels.size();
            int sendMsgCount = 0;
            for (List<Integer> l : sendtoRankToMsgCountAndDestinedVertexLabels.values()){
                sendMsgCount += l.get(0);
            }
            sb.append(recvfromRankCount).append(" ").append(recvMsgCount).append(" ").append(sendtoRankCount).append
                    (" ").append(sendMsgCount);
            String msg = allReduce(sb.toString(), worldProcsComm);
            if (worldProcRank == 0) {
                System.out.println(msg);
            }
        }

        int numOfsendtoRanks = sendtoRankToSendBuffer.containsKey(worldProcRank)
                                ? sendtoRankToSendBuffer.size() - 1
                                : sendtoRankToSendBuffer.size();
        int numOfrecvfromRanks = recvfromRankToRecvBuffer.containsKey(worldProcRank)
                ? recvfromRankToRecvBuffer.size() - 1
                : recvfromRankToRecvBuffer.size();
        recvRequestOffset = numOfsendtoRanks;
        sendRecvRequests = new Request[numOfsendtoRanks+numOfrecvfromRanks];
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

    public static void sendMessages(int msgSize) {
        msgSizeToReceive = msgSize;
        int requestCount = 0;
        for (Map.Entry<Integer, ShortBuffer> kv : sendtoRankToSendBuffer.entrySet()){
            int sendtoRank = kv.getKey();
            ShortBuffer buffer = kv.getValue();
            buffer.put(MSG_SIZE_OFFSET, (short)msgSize);

            try {
                if (sendtoRank == worldProcRank){
                    // local copy
                    ShortBuffer b = recvfromRankToRecvBuffer.get(worldProcRank);
                    b.position(0);
                    buffer.position(0);
                    b.put(buffer);
                } else {

                    converterByteBuffer.putShort(0, buffer.get(MSG_COUNT_OFFSET));
                    converterByteBuffer.putShort(Byte.BYTES, buffer.get(MSG_COUNT_OFFSET+1));
                    int count = BUFFER_OFFSET + converterByteBuffer.getInt(0) * msgSize;
                    if (count <= 0){
                        System.out.println("Invalid Count Error - Rank: " + worldProcRank + "torank: " + sendtoRank +
                                " count: " + count + " msgCount: " + buffer.get(MSG_COUNT_OFFSET) + " msgSize: " + msgSize);
                    }
                    sendRecvRequests[requestCount] = worldProcsComm.iSend(buffer, count, MPI.SHORT, sendtoRank,
                            worldProcRank);
                    ++requestCount;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void recvMessages() throws MPIException {
        int requestCount = 0;
        for (Map.Entry<Integer, ShortBuffer> kv : recvfromRankToRecvBuffer.entrySet()){
            int recvfromRank = kv.getKey();
            ShortBuffer buffer = kv.getValue();
            int msgCount = recvfromRankToMsgCountAndforvertexLabels.get(recvfromRank).get(0);
            try {
                if (recvfromRank != worldProcRank) {
                    sendRecvRequests[requestCount+recvRequestOffset] = worldProcsComm.iRecv(buffer, BUFFER_OFFSET + msgCount *
                                    msgSizeToReceive, MPI.SHORT,
                            recvfromRank, recvfromRank);
                    ++requestCount;
                }
            } catch (MPIException e) {
                e.printStackTrace();
            }
        }

        Request.waitAll(sendRecvRequests);


        // DEBUG
        if (debug) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n%%Rank: ").append(worldProcRank);
            recvfromRankToRecvBuffer.entrySet().forEach(kv -> {
                int recvfromRank = kv.getKey();
                ShortBuffer b = kv.getValue();
                int recvdMsgSize = b.get(MSG_SIZE_OFFSET);
                if (recvdMsgSize != msgSizeToReceive) throw new RuntimeException("recvd msg size " + recvdMsgSize  + " != " +
                        msgSizeToReceive + " msgSize");
                converterByteBuffer.putShort(0, b.get(MSG_COUNT_OFFSET));
                converterByteBuffer.putShort(Byte.BYTES, b.get(MSG_COUNT_OFFSET+1));
                int msgCount = converterByteBuffer.getInt(0);
                sb.append("\n recvd ").append(msgCount).append(" msgs from rank ").append(recvfromRank).append(" of " +
                        "size ").append(recvdMsgSize).append(" msg list: ");
                IntStream.range(0, msgCount).forEach(i -> {
                    sb.append("[");
                    IntStream.range(0, recvdMsgSize).forEach(j -> {
                        sb.append(b.get(BUFFER_OFFSET+(i*recvdMsgSize+j))).append(" ");
                    });
                    sb.append("] ");
                });
            });
            sb.append('\n');
            String msg = allReduce(sb.toString(), worldProcsComm);
            if (worldProcRank == 0) {
                System.out.println(msg);
            }
        }
    }
}
