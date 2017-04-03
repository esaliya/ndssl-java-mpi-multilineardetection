package org.saliya.ndssl.multilinearscan.mpi;

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
    private static boolean bind;
    private static int cps;

    private static GaloisField gf;
    private static ParallelUtils putils;

    static {
        programOptions.addOption(
                String.valueOf(Constants.CMD_OPTION_SHORT_VC),
                Constants.CMD_OPTION_LONG_NUM_VC, true,
                Constants.CMD_OPTION_DESCRIPTION_NUM_VC);
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

        double bestScore = Double.MIN_VALUE;
        long startTime = System.currentTimeMillis();
        initComp(vertices);
        runGraphComp(vertices);
        long endTime = System.currentTimeMillis();
        putils.printMessage("\n== " + Constants.PROGRAM_NAME + " run ended on " + new Date() + " took " + (endTime -
                startTime) + " ms ==\n");
    }

    private static double runGraphComp(Vertex[] vertices) throws MPIException, BrokenBarrierException,
            InterruptedException {

        long startTime = System.currentTimeMillis();
        if (ParallelOps.threadCount > 1) {
            try {
                launchHabaneroApp(() -> forallChunked(0, ParallelOps.threadCount - 1, threadIdx -> {
                    if (bind) {
                        BitSet bitSet = ThreadBitAssigner.getBitSet(ParallelOps.worldProcRank, threadIdx, ParallelOps.threadCount, cps);
                        Affinity.setThreadId();
                        Affinity.setAffinity(bitSet);
                    }
                    try {

                        long t = System.currentTimeMillis();
                        runSuperSteps(vertices, startTime, threadIdx);
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
            runSuperSteps(vertices, startTime, 0);
        }

        return -1;
    }

    private static void runSuperSteps(Vertex[] vertices, long startTime, Integer threadIdx) throws MPIException, BrokenBarrierException, InterruptedException {
    /* Super step loop*/
        int workerSteps = ParallelOps.dagLevels;
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
                ParallelOps.threadComm.cyclicBarrier();
                barrierDuration += (System.currentTimeMillis() - t);

                t = System.currentTimeMillis();
                processRecvdMessages(vertices, ss, threadIdx);
                processRecvdDuration += (System.currentTimeMillis() - t);
            }

            long t = System.currentTimeMillis();
            compute(vertices, ss, threadIdx);
            computeDuration += (System.currentTimeMillis() - t);

            // This barrier is necessary because sendMessages is done by thread0 only
            // and if some threads have not completed compute then prepareSend can't progress
            // The way to avoid this barrier would be to make sendMessages by all threads,
            // so their compute() are known to have finished, but that means
            // we have to change send buffers to support threads.
            // So for now let's try the barrier

            t = System.currentTimeMillis();
            ParallelOps.threadComm.cyclicBarrier();
            barrierDuration += (System.currentTimeMillis() - t);

            t = System.currentTimeMillis();
            if (ss < workerSteps - 1 && threadIdx == 0) {
                sendMessages(vertices, ss);
            }
            sendCommDuration += (System.currentTimeMillis() - t);

            if (ss%10 == 0 || ss == workerSteps-1){
                if (threadIdx == 0) {
                    putils.printMessage("      DAG level " + ss  + " of " + ParallelOps.dagLevels + " " +
                            "elapsed " + (System.currentTimeMillis() - startTime) + " ms");
                }
            }
        }

//        System.out.println("Thread: " + threadIdx + " comp: " + computeDuration + " | recvComm: " +
//                recvCommDuration + " | barrier: " + barrierDuration + " | procRecvd: " + processRecvdDuration +
//                " | sendComm: " + sendCommDuration);


    }

    private static void compute(Vertex[] vertices, int ss, Integer threadIdx) {
        int offset = ParallelOps.threadIdToVertexOffset[threadIdx];
        int count = ParallelOps.threadIdToVertexCount[threadIdx];

        for (int i = 0; i < count; ++i){
            vertices[offset+i].compute(ss);
        }
    }

    private static void initComp(Vertex[] vertices) throws MPIException {
        // TODO - do any initComp
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
                "Parallel Pattern",};
        Object[] args =
                new Object[]{inputFile,
                        globalVertexCount,
                        ParallelOps.threadCount + "x"+(ParallelOps.worldProcsCount/ParallelOps.nodeCount)
                        +"x"+ParallelOps.nodeCount,
                        };

        Optional<Integer> maxLength =
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
