package org.saliya.ndssl.multilinearscan.mpi;

import com.google.common.base.Optional;
import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.saliya.ndssl.Utils;
import org.saliya.ndssl.multilinearscan.GaloisField;
import org.saliya.ndssl.multilinearscan.Polynomial;

import java.util.Date;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/21/17.
 */
public class Program {
    private static Options programOptions = new Options();
    private static String inputFile;
    private static int globalVertexCount;
    private static int k;
    private static double epsilon;
    private static int delta;
    private static double alphaMax;
    private static boolean bind;
    private static int cps;

    private static long mainSeed;
    private static int roundingFactor;
    private static int r;
    private static int twoRaisedToK;
    private static GaloisField gf;
    private static int maxIterations;

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
                String.valueOf(Constants.CMD_OPTION_SHORT_NC),
                Constants.CMD_OPTION_LONG_NC, true,
                Constants.CMD_OPTION_DESCRIPTION_NC);
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_TC),
                Constants.CMD_OPTION_LONG_TC, true,
                Constants.CMD_OPTION_DESCRIPTION_TC);


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

    public static void main(String[] args) throws MPIException {
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
        Vertex[] vertices = ParallelOps.setParallelDecomposition(inputFile, globalVertexCount);

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
        for (Vertex vertex : vertices){
            vertex.processRecvd(superStep, ParallelOps.BUFFER_OFFSET);
        }
    }

    private static void runProgram(Vertex[] vertices) throws MPIException {
        putils = new ParallelUtils(0, ParallelOps.worldProcRank);

        putils.printMessage("\n== " + Constants.PROGRAM_NAME + " run started on " + new Date() + " ==\n");
        // get number of iterations for a target error bound (epsilon)
        double probSuccess = 0.2;
        int iter = (int) Math.round(Math.log(epsilon) / Math.log(1 - probSuccess));
        putils.printMessage(iter + " assignments will be evaluated for epsilon = " + epsilon);
        double roundingFactor = 1 + delta;
        putils.printMessage("approximation factor is " + roundingFactor);

        mainSeed = System.currentTimeMillis();
//        for (int i = 0; i < iter; ++i) {
        for (int i = 0; i < 1; ++i) {
            runGraphComp(vertices);
        }
    }

    private static void runGraphComp(Vertex[] vertices) throws MPIException {
        initComp(vertices);

        /* Super step loop*/
        int MAX_SS = 2;
        for (int ss = 0; ss < MAX_SS; ++ss) {
            if (ss > 0){
                receiveMessages(vertices, ss);
            }

            for (Vertex vertex : vertices) {
                vertex.compute(ss);
            }

            if (ss < MAX_SS - 1){
                sendMessages(vertices, ss);
            }
        }
    }

    private static void initComp(Vertex[] vertices) throws MPIException {
        roundingFactor =  1+delta;
        Random random = new Random(mainSeed);
        // (1 << k) is 2 raised to the kth power
        twoRaisedToK = (1 << k);
        int degree = (3 + Utils.log2(k));
        gf = GaloisField.getInstance(1 << degree, Polynomial.createIrreducible(degree, random).toBigInteger().intValue());
        maxIterations = k-1; // the original pregel loop was from 2 to k (including k), so that's (k-2)+1 times

        if (ParallelOps.worldProcRank == 0){
            Random r = new Random();
            IntStream.range(0, globalVertexCount).forEach(
                    x -> ParallelOps.vertexLongBuffer.put(x, r.nextLong()));
        }
        ParallelOps.worldProcsComm.bcast(
                ParallelOps.vertexLongBuffer, globalVertexCount, MPI.LONG, 0);
        for (int i = 0; i < vertices.length; ++i){
            int offset = ParallelOps.localVertexDisplas[ParallelOps.worldProcRank];
            vertices[i].uniqueRandomSeed = ParallelOps.vertexLongBuffer.get(offset +i);
            // put vertex weights to be collected to compute maxweight - the sum of largest k weights
            ParallelOps.vertexDoubleBuffer.put(offset+i, vertices[i].weight);
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
        putils.printMessage("Max Weight: " + maxWeight + " r: " + r);
        // invalid input: r is negative
        if (r < 0) {
            throw new IllegalArgumentException("r must be a positive integer or 0");
        }


    }


    private static void readConfiguration(CommandLine cmd) {
        inputFile = cmd.hasOption(Constants.CMD_OPTION_SHORT_INPUT) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_INPUT) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_INPUT);
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

        bind = !cmd.hasOption(Constants.CMD_OPTION_SHORT_BIND_THREADS) ||
                Boolean.parseBoolean(cmd.getOptionValue(Constants.CMD_OPTION_SHORT_BIND_THREADS));
        cps = (cmd.hasOption(Constants.CMD_OPTION_SHORT_CPS)) ?
                Integer.parseInt(cmd.getOptionValue(Constants.CMD_OPTION_SHORT_CPS)) :
                -1;
    }
}
