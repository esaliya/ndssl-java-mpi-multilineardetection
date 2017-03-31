package org.saliya.ndssl.multilinearscan.mpi;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Saliya Ekanayake on 3/28/17.
 */
public class ThreadCommunicator {

    private CyclicBarrier cyclicBarrier;

    public ThreadCommunicator(int threadCount) {
        cyclicBarrier = new CyclicBarrier(threadCount);
    }


    public void cyclicBarrier() throws BrokenBarrierException, InterruptedException {
        cyclicBarrier.await();
    }
}
