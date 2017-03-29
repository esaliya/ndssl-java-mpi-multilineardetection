package org.saliya.ndssl.multilinearscan.mpi;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Saliya Ekanayake on 3/28/17.
 */
public class ThreadCommunicator {

    private final int threadCount;
    private final AtomicInteger barrierCounter = new AtomicInteger(0);

    public ThreadCommunicator(int threadCount) {
        this.threadCount = threadCount;
    }

    public void barrier()
            throws BrokenBarrierException, InterruptedException {
        barrierCounter.compareAndSet(threadCount, 0);
        barrierCounter.incrementAndGet();
        while (barrierCounter.get() != threadCount) {
            ;
        }
    }
}
