#!/bin/bash
export TMPDIR=/tmp


#mpirun -np $1 java -cp target/multilineardetection-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.ndssl.multilinearscan.mpi.Program -v 1936 -k 3 -i src/main/resources/2014-01-01_simple.txt -nc 1 -e 0.1 -mms 500 -tc 1 -cps
mpirun -np $1 java -cp target/multilineardetection-1.0-SNAPSHOT-jar-with-dependencies.jar org.saliya.ndssl.multilinearscan.mpi.Program -v 7 -k 3 -i src/main/resources/my_little_graph_simple.txt -nc 1 -e 0.1 -mms 1 -tc 1 -cps 8 -bind false
