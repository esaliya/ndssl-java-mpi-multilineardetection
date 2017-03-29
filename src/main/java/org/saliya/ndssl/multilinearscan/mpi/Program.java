package org.saliya.ndssl.multilinearscan.mpi;

import com.google.common.base.Optional;
import mpi.MPI;
import mpi.MPIException;
import net.openhft.affinity.Affinity;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.saliya.ndssl.Utils;
import org.saliya.ndssl.multilinearscan.GaloisField;
import org.saliya.ndssl.multilinearscan.Polynomial;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.stream.IntStream;

import static edu.rice.hj.Module0.launchHabaneroApp;
import static edu.rice.hj.Module1.forallChunked;

/**
 * Saliya Ekanayake on 2/21/17.
 */
public class Program {
    private static Options programOptions = new Options();
    private static String inputFile;
    private static String partsFile;
    private static int globalVertexCount;
    private static int k;
    private static double epsilon;
    private static int delta;
    private static double alphaMax;
    private static boolean bind;
    private static int cps;

    private static int roundingFactor;
    private static int r;
    private static int twoRaisedToK;
    private static GaloisField gf;
    private static int maxIterations;
    private static TreeMap<Integer, Integer> randomAssignments;
    private static int[] completionVariables;

    private static ParallelUtils putils;

    static {
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_VC),
                Constants.CMD_OPTION_LONG_NUM_VC, true,
                Constants.CMD_OPTION_DESCRIPTION_NUM_VC);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_K),
                Constants.CMD_OPTION_LONG_K, true,
                Constants.CMD_OPTION_DESCRIPTION_K);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_DELTA),
                Constants.CMD_OPTION_LONG_DELTA, true,
                Constants.CMD_OPTION_DESCRIPTION_DELTA);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_ALPHA),
                Constants.CMD_OPTION_LONG_ALPHA, true,
                Constants.CMD_OPTION_DESCRIPTION_ALPHA);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_EPSILON),
                Constants.CMD_OPTION_LONG_EPSILON, true,
                Constants.CMD_OPTION_DESCRIPTION_EPSILON);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_INPUT),
                Constants.CMD_OPTION_LONG_INPUT, true,
                Constants.CMD_OPTION_DESCRIPTION_INPUT);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_PARTS),
                Constants.CMD_OPTION_LONG_PARTS, true,
                Constants.CMD_OPTION_DESCRIPTION_PARTS);

        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_NC),
                Constants.CMD_OPTION_LONG_NC, true,
                Constants.CMD_OPTION_DESCRIPTION_NC);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_TC),
                Constants.CMD_OPTION_LONG_TC, true,
                Constants.CMD_OPTION_DESCRIPTION_TC);

        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_MMS),
                Constants.CMD_OPTION_LONG_MMS, true,
                Constants.CMD_OPTION_DESCRIPTION_MMS);


        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_MMAP_SCRATCH_DIR, true,
                Constants.CMD_OPTION_DESCRIPTION_MMAP_SCRATCH_DIR);
        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_BIND_THREADS, true,
                Constants.CMD_OPTION_DESCRIPTION_BIND_THREADS);
        programOptions.addOption(
                Constants.CMD_OPTION_SHORT_CPS, true,
                Constants.CMD_OPTION_DESCRIPTION_CPS);
    }

    public static void main(String[] args) throws MPIException, BrokenBarrierException, InterruptedException {
        Optional<CommandLine> parserResult =
                Utils.parseCommandLineArguments(args, programOptions);

        if (!parserResult.isPresent()) {
            System.out.println(Constants.ERR_PROGRAM_ARGUMENTS_PARSING_FAILED);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        CommandLine cmd = parserResult.get();
        if (!((cmd.hasOption(Constants.CMD_OPTION_SHORT_VC)||cmd.hasOption(Constants.CMD_OPTION_LONG_NUM_VC)) &&
                (cmd.hasOption(Constants.CMD_OPTION_SHORT_K)||cmd.hasOption(Constants.CMD_OPTION_LONG_K)) &&
                (cmd.hasOption(Constants.CMD_OPTION_SHORT_INPUT)||cmd.hasOption(Constants.CMD_OPTION_LONG_INPUT)) &&
                (cmd.hasOption(Constants.CMD_OPTION_SHORT_NC)||cmd.hasOption(Constants.CMD_OPTION_LONG_NC)))) {
            System.out.println(Constants.ERR_INVALID_PROGRAM_ARGUMENTS);
            new HelpFormatter()
                    .printHelp(Constants.PROGRAM_NAME, programOptions);
            return;
        }

        readConfiguration(cmd);

        ParallelOps.setupParallelism(args);
        Vertex[] vertices = ParallelOps.setParallelDecomposition(inputFile, globalVertexCount, partsFile);

        runProgram(vertices);

        ParallelOps.tearDownParallelism();
    }

    private static void sendMessages(Vertex[] vertices, int superStep) {
        int msgSize = -1;
        for (Vertex vertex : vertices){
            msgSize = vertex.prepareSend(superStep, ParallelOps.BUFFER_OFFSET);
        }
        ParallelOps.sendMessages(msgSize);

    }


    private static void receiveMessages(Vertex[] vertices, int superStep) throws MPIException {
        ParallelOps.recvMessages();
    }

    private static void processRecvdMessages(Vertex[] vertices, int superStep, Integer threadIdx) {
        int offset = ParallelOps.threadIdToVertexOffset[threadIdx];
        int count = ParallelOps.threadIdToVertexCount[threadIdx];
        for (int i = 0; i < count; ++i){
            vertices[offset+i].processRecvd(superStep, ParallelOps.BUFFER_OFFSET);
        }
    }

    private static void runProgram(Vertex[] vertices) throws MPIException, BrokenBarrierException, InterruptedException {
        putils = new ParallelUtils(0, ParallelOps.worldProcRank);

        putils.printMessage("\n== " + Constants.PROGRAM_NAME + " run started on " + new Date() + " ==\n");
        putils.printMessage(configPrettyString(true));
        // get number of iterations for a target error bound (epsilon)
        double probSuccess = 0.2;
        int iter = (int) Math.round(Math.log(epsilon) / Math.log(1 - probSuccess));
        putils.printMessage("  " + iter + " assignments will be evaluated for epsilon = " + epsilon);
        double roundingFactor = 1 + delta;
        putils.printMessage("  approximation factor is " + roundingFactor);


        double bestScore = Double.MIN_VALUE;
        long startTime = System.currentTimeMillis();
        initComp(vertices);
        //for (int i = 0; i < iter; ++i) {
        for (int i = 0; i < 1; ++i) {
            putils.printMessage("  Start of Loop: " + i);
            long loopStartTime = System.currentTimeMillis();
            bestScore = Math.max(bestScore, runGraphComp(i, vertices));
            putils.printMessage("  End of Loop: " + i + " took " + (System.currentTimeMillis() - loopStartTime) + " " +
                    "ms");
        }
        long endTime = System.currentTimeMillis();
        putils.printMessage("Best score: " + bestScore);
        putils.printMessage("\n== " + Constants.PROGRAM_NAME + " run ended on " + new Date() + " took " + (endTime -
                startTime) + " ms ==\n");
    }

    private static double runGraphComp(int loopNumber, Vertex[] vertices) throws MPIException, BrokenBarrierException, InterruptedException {
        initLoop(vertices);

        long startTime = System.currentTimeMillis();
        //for (int iter = 0; iter < twoRaisedToK; ++iter) {
        for (int iter = 0; iter < 3; ++iter) {
            int finalIter = iter;
            if (ParallelOps.threadCount > 1) {
                try {
                    launchHabaneroApp(() -> forallChunked(0, ParallelOps.threadCount - 1, threadIdx -> {
                        if (bind) {
                            BitSet bitSet = ThreadBitAssigner.getBitSet(ParallelOps.worldProcRank, threadIdx, ParallelOps.threadCount, cps);
                            Affinity.setThreadId();
//                            System.out.println("Thread: " + threadIdx + " id: " + Affinity.getThreadId() + " affinity" +
//                                    " " + bitSet);
                            Affinity.setAffinity(bitSet);
                        }
                        try {

                            long t = System.currentTimeMillis();
                            runSuperSteps(vertices, startTime, finalIter, threadIdx);
//                            System.out.printf("Thread: " + threadIdx + " time with comm " + (System.currentTimeMillis
//                                    () - t) + " ms");

                        } catch (MPIException | InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        }
                    }));
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            } else {
                runSuperSteps(vertices, startTime, finalIter, 0);
            }
        }
        double bestScore = finalizeIterations(vertices);
        ParallelOps.oneDoubleBuffer.put(0, bestScore);
        ParallelOps.worldProcsComm.allReduce(ParallelOps.oneDoubleBuffer, 1, MPI.DOUBLE, MPI.MAX);
        bestScore = ParallelOps.oneDoubleBuffer.get(0);
        putils.printMessage("    Loop "  +loopNumber + " best score: " + bestScore);
        return bestScore;
    }

    private static void runSuperSteps(Vertex[] vertices, long startTime, int iter, Integer threadIdx) throws MPIException, BrokenBarrierException, InterruptedException {
    /* Super step loop*/
        int workerSteps = maxIterations+1; // +1 to send initial values
        long computeDuration = 0;
        long recvCommDuration = 0;
        long barrierDuration = 0;
        long processRecvdDuration = 0;
        long sendCommDuration = 0;
        for (int ss = 0; ss < workerSteps; ++ss) {
            if (ss > 0) {
                if (threadIdx == 0) {
                    long t = System.currentTimeMillis();
                    receiveMessages(vertices, ss);
                    recvCommDuration += (System.currentTimeMillis() - t);
                }
                long t = System.currentTimeMillis();
                ParallelOps.threadComm.barrier();
                barrierDuration += (System.currentTimeMillis() - t);

                t = System.currentTimeMillis();
                processRecvdMessages(vertices, ss, threadIdx);
                processRecvdDuration += (System.currentTimeMillis() - t);
            }

            long t = System.currentTimeMillis();
            compute(iter, vertices, ss, threadIdx);
            computeDuration += (System.currentTimeMillis() - t);

            t = System.currentTimeMillis();
            if (ss < workerSteps - 1 && threadIdx == 0) {
                sendMessages(vertices, ss);
            }
            sendCommDuration += (System.currentTimeMillis() - t);
        }

        long t = System.currentTimeMillis();
        finalizeIteration(vertices, threadIdx);
        computeDuration += System.currentTimeMillis() - t;

        System.out.println("Thread: " + threadIdx + " comp: " + computeDuration + " | recvComm: " +
                recvCommDuration + " | barrier: " + barrierDuration + " | procRecvd: " + processRecvdDuration +
                " | sendComm: " + sendCommDuration);

        if (iter%10 == 0 || iter == twoRaisedToK-1){
            if (threadIdx == 0) {
                putils.printMessage("      Iteration " + (iter+1)  + " of " + twoRaisedToK + " " +
                        "elapsed " +
                        (System
                                .currentTimeMillis() - startTime) + " ms");
            }
        }
    }

    private static double finalizeIterations(Vertex[] vertices) {
        double bestScore = Double.MIN_VALUE;
        for (Vertex vertex : vertices){
            bestScore = Math.max(bestScore, vertex.finalizeIterations(alphaMax, roundingFactor));
        }
        return bestScore;
    }

    private static void finalizeIteration(Vertex[] vertices, int threadIdx) {
        int offset = ParallelOps.threadIdToVertexOffset[threadIdx];
        int count = ParallelOps.threadIdToVertexCount[threadIdx];
        for (int i = 0; i < count; ++i){
            vertices[offset+i].finalizeIteration();
        }
    }

    private static void compute(int iter, Vertex[] vertices, int ss, Integer threadIdx) {
        int offset = ParallelOps.threadIdToVertexOffset[threadIdx];
        int count = ParallelOps.threadIdToVertexCount[threadIdx];
        for (int i = 0; i < count; ++i){
            vertices[offset+i].compute(ss, iter, completionVariables, randomAssignments);
        }
        /*for (Vertex vertex : vertices) {
            vertex.compute(ss, iter, completionVariables, randomAssignments);
        }*/
    }

    private static void initComp(Vertex[] vertices) throws MPIException {
        roundingFactor =  1+delta;
        // (1 << k) is 2 raised to the kth power
        twoRaisedToK = (1 << k);
        maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times


        int myDisplas = ParallelOps.localVertexDisplas[ParallelOps.worldProcRank];
        // same as vertices.length
        int myLength = ParallelOps.localVertexCounts[ParallelOps.worldProcRank];
        for (int i = 0; i < myLength; ++i){
            ParallelOps.vertexDoubleBuffer.put(myDisplas+i, vertices[i].weight);
            vertices[i].weight = (int) Math.ceil(
                    Utils.logb((int) vertices[i].weight + 1,
                            roundingFactor));
        }

        ParallelOps.worldProcsComm.allGatherv(ParallelOps.vertexDoubleBuffer, ParallelOps.localVertexCounts,
                ParallelOps.localVertexDisplas, MPI.DOUBLE);

        PriorityQueue<Double> pq = new PriorityQueue<>();
        for (int i = 0; i < globalVertexCount; ++i){
            double val = ParallelOps.vertexDoubleBuffer.get(i);
            if (i < k){
                pq.add(val);
            } else {
                if (pq.peek() >= val) continue;
                if (pq.size() == k) {
                    pq.poll();
                }
                pq.offer(val);
            }
        }
        double maxWeight = 0;
        for (double d : pq){
            maxWeight+=d;
        }

        r = (int) Math.ceil(Utils.logb((int) maxWeight + 1, roundingFactor));
        putils.printMessage("  Max Weight: " + maxWeight + " r: " + r + "\n");
        // invalid input: r is negative
        if (r < 0) {
            throw new IllegalArgumentException("  r must be a positive integer or 0");
        }
    }

    private static void initLoop(Vertex[] vertices) throws MPIException {
        long perLoopRandomSeed = System.currentTimeMillis();
        if (ParallelOps.worldProcRank == 0) {
            ParallelOps.oneLongBuffer.put(0, perLoopRandomSeed);
        }
        ParallelOps.worldProcsComm.bcast(ParallelOps.oneLongBuffer, 1, MPI.LONG, 0);
        perLoopRandomSeed = ParallelOps.oneLongBuffer.get(0);

        Random random = new Random(perLoopRandomSeed);
        int degree = (3 + Utils.log2(k));
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, random).toBigInteger().intValue());

        int myDisplas = ParallelOps.localVertexDisplas[ParallelOps.worldProcRank];
        // same as vertices.length
        int myLength = ParallelOps.localVertexCounts[ParallelOps.worldProcRank];
        for (int i = 0; i < globalVertexCount; ++i){
            long uniqRandomVal = random.nextLong();
            if (i >= myDisplas && i < myDisplas+myLength){
                vertices[i-myDisplas].uniqueRandomSeed = uniqRandomVal;
                // put vertex weights to be collected to compute maxweight - the sum of largest k weights
            }
        }

        // DEBUG - check each vertex has a unique random seed
        /*StringBuffer sb = new StringBuffer("Rank ");
        sb.append(ParallelOps.worldProcRank).append('\n');
        for (Vertex vertex : vertices){
            sb.append(vertex.uniqueRandomSeed).append(" ");
        }
        System.out.println(sb.toString());*/

        randomAssignments = new TreeMap<>();
        ParallelOps.vertexLabelToWorldRank.keySet().forEach(
                vertexLabel -> randomAssignments.put(
                        vertexLabel, random.nextInt(twoRaisedToK)));
        completionVariables = new int[k-1];
        IntStream.range(0,k-1).forEach(i->completionVariables[i] = random.nextInt(twoRaisedToK));
        for (Vertex vertex : vertices) {
            vertex.init(k, r, gf);
        }
    }


    private static void readConfiguration(CommandLine cmd) {
        inputFile = cmd.hasOption(Constants.CMD_OPTION_SHORT_INPUT) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_INPUT) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_INPUT);
        partsFile = cmd.hasOption(Constants.CMD_OPTION_SHORT_PARTS) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_PARTS) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_PARTS);

        globalVertexCount = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_VC) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_VC) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_NUM_VC));
        k = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_K) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_K) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_K));
        epsilon = Double.parseDouble(cmd.hasOption(Constants.CMD_OPTION_SHORT_EPSILON) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_EPSILON) :
                (cmd.hasOption(Constants.CMD_OPTION_LONG_EPSILON) ? cmd.getOptionValue(Constants
                .CMD_OPTION_LONG_EPSILON) : "1.0"));
        delta = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_DELTA) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_DELTA) :
                (cmd.hasOption(Constants.CMD_OPTION_LONG_DELTA) ? cmd.getOptionValue(Constants
                        .CMD_OPTION_LONG_DELTA) : "1"));
        alphaMax = Double.parseDouble(cmd.hasOption(Constants.CMD_OPTION_SHORT_ALPHA) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_ALPHA) :
                (cmd.hasOption(Constants.CMD_OPTION_LONG_ALPHA) ? cmd.getOptionValue(Constants
                        .CMD_OPTION_LONG_ALPHA) : "0.15"));

        ParallelOps.nodeCount = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_NC) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_NC) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_NC));
        ParallelOps.threadCount = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_TC) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_TC) :
                (cmd.hasOption(Constants.CMD_OPTION_LONG_TC) ? cmd.getOptionValue(Constants
                        .CMD_OPTION_LONG_TC) : "1"));
        ParallelOps.mmapScratchDir = cmd.hasOption(Constants.CMD_OPTION_SHORT_MMAP_SCRATCH_DIR) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_MMAP_SCRATCH_DIR) : "/tmp";

        ParallelOps.MAX_MSG_SIZE = cmd.hasOption(Constants.CMD_OPTION_SHORT_MMS)
                ? Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_SHORT_MMS))
                : cmd.hasOption(Constants.CMD_OPTION_LONG_MMS)
                ? Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_LONG_MMS))
                : 500;

        bind = !cmd.hasOption(Constants.CMD_OPTION_SHORT_BIND_THREADS) ||
                Boolean.parseBoolean(cmd.getOptionValue(Constants.CMD_OPTION_SHORT_BIND_THREADS));
        cps = (cmd.hasOption(Constants.CMD_OPTION_SHORT_CPS)) ?
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_SHORT_CPS)) :
                -1;
    }

    public static String configPrettyString(boolean centerAligned) {
        String[] params = {"Input File",
                "Global Vertex Count",
                "K",
                "Epsilon",
                "Delta",
                "Alpha Max",
                "Parallel Pattern",};
        Object[] args =
                new Object[]{inputFile,
                        globalVertexCount,
                        k,
                        epsilon,
                        delta,
                        alphaMax,
                        ParallelOps.threadCount + "x"+(ParallelOps.worldProcsCount/ParallelOps.nodeCount)
                        +"x"+ParallelOps.nodeCount,
                        };

        java.util.Optional<Integer> maxLength =
                Arrays.stream(params).map(String::length).reduce(Math::max);
        if (!maxLength.isPresent()) { return ""; }
        final int max = maxLength.get();
        final String prefix = "  ";
        StringBuilder sb = new StringBuilder("Parameters...\n");
        if (centerAligned) {
            IntStream.range(0, params.length).forEach(
                    i -> {
                        String param = params[i];
                        sb.append(getPadding(max - param.length(), prefix))
                                .append(param).append(": ").append(args[i]).append('\n');
                    });
        }
        else {
            IntStream.range(0, params.length).forEach(
                    i -> {
                        String param = params[i];
                        sb.append(prefix).append(param).append(':')
                                .append(getPadding(max - param.length(), ""))
                                .append(args[i]).append('\n');
                    });
        }
        return sb.toString();
    }

    private static String getPadding(int count, String prefix){
        StringBuilder sb = new StringBuilder(prefix);
        IntStream.range(0,count).forEach(i -> sb.append(' '));
        return sb.toString();
    }
}
