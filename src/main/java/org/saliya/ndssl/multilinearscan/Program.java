package org.saliya.ndssl.multilinearscan;

import com.google.common.base.Optional;
import mpi.MPIException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.saliya.ndssl.Utils;

import java.util.Hashtable;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Saliya Ekanayake on 2/21/17.
 */
public class Program {
    private static Options programOptions = new Options();
    private static String inputFile;
    private static int vertexCount;
    private static int k;
    private static int epsilon;
    private static int delta;
    private static double alphaMax;
    private static boolean bind;
    private static int cps;

    private static Vertex[] vertices;

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
        vertices = ParallelOps.setParallelDecomposition(inputFile, vertexCount);

        /* Super step loop*/
        int MAX_SS = 2;
        for (int ss = 0; ss < MAX_SS; ++ss) {
            int msgSize = 1;// has to be decided per super step
            if (ss > 0){
                receiveMessages(msgSize);
            }

            for (Vertex vertex : vertices) {
                vertex.compute();
            }

            if (ss < MAX_SS - 1){
                sendMessages(vertices, msgSize);
            }
        }

        // DEBUG
        //System.out.println("Rank: " + ParallelOps.worldProcRank + " startVertexID: " + vertices[0].vertexId + " " +
        //        "startVertexLabel: " + vertices[0].vertexLabel + " endVertexID: " + vertices[vertices.length - 1]
        //        .vertexId + " endVertexLabel: " + vertices[vertices.length - 1].vertexLabel);

        runProgram();

        ParallelOps.tearDownParallelism();
    }

    private static void sendMessages(Vertex[] vertices, int msgSize) {
        for (Vertex vertex : vertices){

            vertex.outrankToSendBuffer.entrySet().forEach(kv ->{
                VertexBuffer vertexBuffer = kv.getValue();
                int offset = ParallelOps.BUFFER_OFFSET + vertexBuffer.offsetFactor * vertex.msgSize;
                IntStream.range(0, vertex.msgSize).forEach(i ->{
                    vertexBuffer.buffer.put(offset+i, vertex.message.get(i));
                });
            });
        }
        ParallelOps.sendMessages(msgSize);
    }


    private static void receiveMessages(int msgSize) throws MPIException {
        ParallelOps.recvMessages(msgSize);
    }

    private static void runProgram() {
    }



    private static void readConfiguration(CommandLine cmd) {
        inputFile = cmd.hasOption(Constants.CMD_OPTION_SHORT_INPUT) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_INPUT) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_INPUT);
        vertexCount = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_VC) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_VC) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_NUM_VC));
        k = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_K) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_K) :
                cmd.getOptionValue(Constants.CMD_OPTION_LONG_K));
        epsilon = Integer.parseInt(cmd.hasOption(Constants.CMD_OPTION_SHORT_EPSILON) ?
                cmd.getOptionValue(Constants.CMD_OPTION_SHORT_EPSILON) :
                (cmd.hasOption(Constants.CMD_OPTION_LONG_EPSILON) ? cmd.getOptionValue(Constants
                .CMD_OPTION_LONG_EPSILON) : "1"));
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
