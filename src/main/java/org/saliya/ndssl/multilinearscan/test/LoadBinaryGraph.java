package org.saliya.ndssl.multilinearscan.test;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Saliya Ekanayake on 7/30/17.
 */
public class LoadBinaryGraph {
    public static void main(String[] args) {
        int globalVertexCount = Integer.parseInt(args[0]);
        int globalEdgeCount = Integer.parseInt(args[1]);
        String file = args[2];

        long t = System.currentTimeMillis();
        try (FileChannel fc = (FileChannel) Files.newByteChannel(Paths.get(
                file), StandardOpenOption.READ)) {
            MappedByteBuffer headerMap;
            MappedByteBuffer dataMap;

            long headerExtent = ((long) globalVertexCount) * 2 * Integer.BYTES;
            headerMap = fc.map(FileChannel.MapMode.READ_ONLY, 0, headerExtent);

            for (int i = 0; i < 10; ++i){
                System.out.println("val " + i + " " + headerMap.getInt());
            }

//            long dataOffset = 0;
//            long dataExtent = 0;
//            for (int i = 0; i < globalVertexCount; ++i){
//                if (skipVertexCount == i){
//                    break;
//                }
//                headerMap.getInt();//skip-node id
//                // skip-node's weight+neighbors
//                // +1 because we store node id as well
//                dataOffset += (long)(headerMap.getInt()+1);
//            }
//            dataOffset *= Integer.BYTES;
//
//            int[] vertexNeighborLength = new int[myVertexCount];
//            int[] outNeighbors = new int[globalVertexCount];
//            long runningExtent;
//            long readExtent = 0L;
//            int readVertex = -1;
//            for (int i = 0; i < myVertexCount; ++i){
//                headerMap.getInt();// my ith vertex's node id
//                // my ith vertex's weight+neighbors
//                //+ +1 because we store node id as well
//                int len = headerMap.getInt();
//                vertexNeighborLength[i] = len-1;
//
//                runningExtent = dataExtent + (long)(len +1);
//                if (runningExtent*Integer.BYTES <= Integer.MAX_VALUE){
//                    dataExtent = runningExtent;
//                } else {
//                    dataExtent = readVertices(vertices, skipVertexCount, fc, headerExtent, dataOffset,
//                            dataExtent, vertexNeighborLength, outNeighbors, readExtent, readVertex, i);
//                    readExtent += dataExtent;
//                    dataExtent = (long)(len +1);
//                    readVertex = (i-1);
//                }
//            }
//            readVertices(vertices, skipVertexCount, fc, headerExtent, dataOffset,
//                    dataExtent, vertexNeighborLength, outNeighbors, readExtent, readVertex, myVertexCount);
        } catch (IOException e) {
//            System.out.println("***ERROR: Rank: " + worldProcRank + " of " + worldProcsCount + " " + e.getMessage());
        }
//        if (debug3){
//            System.out.println("Rank: " + worldProcRank + " readgraph: "+ (System.currentTimeMillis() - t) + " ms");
//        }

//        t = System.currentTimeMillis();
//        findNeighbors(globalVertexCount, vertices);
//        if (debug3 && worldProcRank == 0){
//            System.out.println("Rank: 0 findNbrs: "+ (System.currentTimeMillis() - t) + " ms");
//        }
    }
}
