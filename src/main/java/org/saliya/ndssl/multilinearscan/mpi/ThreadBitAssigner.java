package org.saliya.ndssl.multilinearscan.mpi;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Pattern;

public class ThreadBitAssigner {
    private int spn; // sockets per node
    private int cps; // cores per socket
    private int htpc; // hyper threads per core
    private int tpp; // threads per process
    private String rankFile; // rank file
    private boolean useRankFile = false;
    private Hashtable<Integer, List<Integer>> rankToCoreIds = new Hashtable<>();

    public ThreadBitAssigner(int spn, int cps, int htpc, int tpp) {
        this.spn = spn;
        this.cps = cps;
        this.htpc = htpc;
        this.tpp = tpp;
    }

    public ThreadBitAssigner(int spn, int cps, int htpc, String rankFile) {
        this.spn = spn;
        this.cps = cps;
        this.htpc = htpc;
        this.rankFile = rankFile;
        useRankFile = (!Strings.isNullOrEmpty(rankFile) && new File(rankFile).exists());
        if(useRankFile){
            readRankFile(rankFile);
        }
    }

    private void readRankFile(String rankFile) {
        String pat = "[ =,]";
        Pattern sep = Pattern.compile(pat);
        try(BufferedReader reader = Files.newBufferedReader(Paths.get(rankFile))){
            String line;
            while (!Strings.isNullOrEmpty(line = reader.readLine())){
                String[] splits = sep.split(line);
                int rank = Integer.parseInt(splits[1]);
                List<Integer> coreIds = new ArrayList<>();
                for (int i = 4; i < splits.length; ++i){
                    coreIds.add(Integer.parseInt(splits[i]));
                }
                rankToCoreIds.put(rank, coreIds);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int rank = 0;
        int threadIdx = 3;

        int spn=2;
        int cps=12;
        int htpc=2;
        int tpp = 8;
        ThreadBitAssigner tba = new ThreadBitAssigner(spn, cps, htpc, tpp);
        BitSet bitSet = tba.getBitSet(rank, threadIdx);
        System.out.println(bitSet);

        tba = new ThreadBitAssigner(spn, cps, htpc, "src/main/resources/rankfile.txt");
        bitSet = tba.getBitSet(rank, threadIdx);
        System.out.println(bitSet);
    }
    private int[] getBitMask(int rank, int threadIdx){
        int[] bitset = new int[htpc];
        int cpn = cps * spn;
        if (!useRankFile) {
            int ppn = cpn / tpp; // process per node

            // Assuming continuous ranking within a node
            int nodeLocalRank = rank % ppn;

            for (int j = 0; j < htpc; ++j) {
                bitset[j] = nodeLocalRank * tpp + threadIdx + (cpn * j);
            }
        } else {
            int coreId = rankToCoreIds.get(rank).get(threadIdx);
            for (int j = 0; j < htpc; ++j) {
                bitset[j] = coreId + (cpn * j);
            }
        }
        return bitset;
    }

    public BitSet getBitSet(int rank, int threadIdx){
        int cun = htpc*cps*spn; // computing units per node (including hts)
        int[] bitMask = getBitMask(rank, threadIdx);
        BitSet bitSet = new BitSet(cun);
        for (int mask : bitMask) {
            bitSet.set(mask);
        }
        return bitSet;
    }
}
