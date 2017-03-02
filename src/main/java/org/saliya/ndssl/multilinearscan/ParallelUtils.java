package org.saliya.ndssl.multilinearscan;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class ParallelUtils {
    int threadId = 0;
    int worldProcRank = 0;

    public void printMessage(String message){
        if (worldProcRank == 0 && threadId == 0){
            System.out.println(message);
        }
    }
}
