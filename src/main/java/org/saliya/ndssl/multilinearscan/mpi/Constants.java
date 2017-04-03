package org.saliya.ndssl.multilinearscan.mpi;

import org.apache.commons.cli.Options;

/**
 * Saliya Ekanayake on 2/22/17.
 */
public class Constants {

    public static String PROGRAM_NAME = "MultilinearScan";

    static final String CMD_OPTION_SHORT_VC = "v";
    static final String CMD_OPTION_LONG_NUM_VC = "vertexCount";
    static final String CMD_OPTION_DESCRIPTION_NUM_VC = "Vertex count";

    static final String CMD_OPTION_SHORT_K = "k";
    static final String CMD_OPTION_LONG_K = "k";
    static final String CMD_OPTION_DESCRIPTION_K = "Motif size";

    static final String CMD_OPTION_SHORT_DELTA = "d";
    static final String CMD_OPTION_LONG_DELTA = "delta";
    static final String CMD_OPTION_DESCRIPTION_DELTA = "Delta";

    static final String CMD_OPTION_SHORT_ALPHA = "a";
    static final String CMD_OPTION_LONG_ALPHA = "alpha";
    static final String CMD_OPTION_DESCRIPTION_ALPHA = "Alpha max";

    static final String CMD_OPTION_SHORT_EPSILON = "e";
    static final String CMD_OPTION_LONG_EPSILON = "epsilon";
    static final String CMD_OPTION_DESCRIPTION_EPSILON = "Epsilon";

    static final String CMD_OPTION_SHORT_INPUT = "i";
    static final String CMD_OPTION_LONG_INPUT = "input";
    static final String CMD_OPTION_DESCRIPTION_INPUT = "Input file";

    static final String CMD_OPTION_SHORT_PARTS = "p";
    static final String CMD_OPTION_LONG_PARTS = "parts";
    static final String CMD_OPTION_DESCRIPTION_PARTS = "Partition file";

    static final String CMD_OPTION_SHORT_NC = "nc";
    static final String CMD_OPTION_LONG_NC = "nodeCount";
    static final String CMD_OPTION_DESCRIPTION_NC = "Node count";

    static final String CMD_OPTION_SHORT_TC = "tc";
    static final String CMD_OPTION_LONG_TC = "threadCount";
    static final String CMD_OPTION_DESCRIPTION_TC = "Thread count";

    static final String CMD_OPTION_SHORT_MMS = "mms";
    static final String CMD_OPTION_LONG_MMS = "maxMsgSize";
    static final String CMD_OPTION_DESCRIPTION_MMS = "Maximum message size (#shorts)";

    static final String CMD_OPTION_SHORT_PI = "pi";
    static final String CMD_OPTION_LONG_PI = "parallelInstance";
    static final String CMD_OPTION_DESCRIPTION_PI = "Parallel instance";

    static final String CMD_OPTION_SHORT_PIC = "pic";
    static final String CMD_OPTION_LONG_PIC = "parallelInstanceCount";
    static final String CMD_OPTION_DESCRIPTION_PIC = "Parallel instance count";

    static final String CMD_OPTION_SHORT_RF = "rf";
    static final String CMD_OPTION_LONG_RF = "rankFile";
    static final String CMD_OPTION_DESCRIPTION_RF = "Rank file";

    public static final String CMD_OPTION_SHORT_MMAP_SCRATCH_DIR = "mmapdir";
    public static final String
            CMD_OPTION_DESCRIPTION_MMAP_SCRATCH_DIR =
            "Scratch directory to store memmory mapped files. A node local "
                    + "volatile storage like tmpfs is advised for this";

    public static final String CMD_OPTION_SHORT_BIND_THREADS = "bind";
    public static final String CMD_OPTION_DESCRIPTION_BIND_THREADS = "Bind threads to cores";

    public static final String CMD_OPTION_SHORT_CPS = "cps";
    public static final String CMD_OPTION_DESCRIPTION_CPS = "Cores per socket";

    public static final String CMD_OPTION_SHORT_SPN = "spn";
    public static final String CMD_OPTION_DESCRIPTION_SPN = "Sockets per node";

    public static final String CMD_OPTION_SHORT_HTPC = "htpc";
    public static final String CMD_OPTION_DESCRIPTION_HTPC = "Hyper threads per core";



    static final String
            ERR_PROGRAM_ARGUMENTS_PARSING_FAILED =
            "Argument parsing failed!";
    static final String
            ERR_INVALID_PROGRAM_ARGUMENTS =
            "Invalid program arguments!";
    static final String ERR_EMPTY_FILE_NAME = "File name is null or empty!";

}
