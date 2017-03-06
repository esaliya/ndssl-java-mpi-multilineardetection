package org.saliya.ndssl.multilinearscan.mpi;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class ParallelUtils {
    int threadId = 0;
    int worldProcRank = 0;

    public ParallelUtils(int threadId, int worldProcRank) {
        this.threadId = threadId;
        this.worldProcRank = worldProcRank;
    }

    public void printMessage(String message){
        if (worldProcRank == 0 && threadId == 0){
            System.out.println(message);
        }
    }
}
