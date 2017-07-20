package org.saliya.ndssl.multilinearscan.test;

import com.google.common.base.Strings;
import org.saliya.ndssl.multilinearscan.mpi.Vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Saliya Ekanayake on 7/7/17.
 */
public class LoadGraph {
    public static void main(String[] args) {
        String graphFile = "/Users/esaliya/sali/projects/graphs/data/snap/fascia_com-orkut.ungraph.txt";
        int n,m;
        int[] srcs, dsts;

        Pattern pat = Pattern.compile("\t");

        long t = System.currentTimeMillis();
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(graphFile))){
            String line = reader.readLine();
            n = Integer.parseInt(line);
            line = reader.readLine();
            m = Integer.parseInt(line);
            srcs = new int[m];
            dsts = new int[m];
            String[] splits;
            for (int i = 0; i < m; ++i){
                line = reader.readLine();
                splits = pat.split(line);
                srcs[i] = Integer.parseInt(splits[0]);
                dsts[i] = Integer.parseInt(splits[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("*****Rank: 0 finished reading the whole graph in " + ((System.currentTimeMillis() - t)
                /1000.0) + "seconds");

    }
}
