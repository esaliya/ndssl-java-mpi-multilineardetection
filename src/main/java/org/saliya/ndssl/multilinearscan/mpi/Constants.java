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

    public static final String CMD_OPTION_SHORT_MMAP_SCRATCH_DIR = "mmapdir";
    public static final String
            CMD_OPTION_DESCRIPTION_MMAP_SCRATCH_DIR =
            "Scratch directory to store memmory mapped files. A node local "
                    + "volatile storage like tmpfs is advised for this";

    public static final String CMD_OPTION_SHORT_BIND_THREADS = "bind";
    public static final String CMD_OPTION_DESCRIPTION_BIND_THREADS = "Bind threads to cores";

    public static final String CMD_OPTION_SHORT_CPS = "cps";
    public static final String CMD_OPTION_DESCRIPTION_CPS = "Cores per socket";



    static final String
            ERR_PROGRAM_ARGUMENTS_PARSING_FAILED =
            "Argument parsing failed!";
    static final String
            ERR_INVALID_PROGRAM_ARGUMENTS =
            "Invalid program arguments!";
    static final String ERR_EMPTY_FILE_NAME = "File name is null or empty!";

}
