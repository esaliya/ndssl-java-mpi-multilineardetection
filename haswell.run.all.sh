#!/bin/bash

:<<COMMENT
fn=2014-01-01_simple
i=/N/u/sekanaya/sali/git/github/esaliya/java/ndssl-java-mpi-multilineardetection/src/main/resources/"$fn".txt
v=1936
COMMENT

:<<COMMENT
fn=soc-LiveJournal1-refined
v=4843864
dir=/N/u/sekanaya/sali/projects/vt/giraph
COMMENT

fn=0100000
v=80991
#fn=1000000
#v=809165
dir=/localscratch/esaliya/sali/projects/vt/giraph/random-er

i=$dir/"$fn".txt
k=6
e=0.1

nodes=2
ppn=16
xmx=4g

cps=16
spn=2
nodefile=nodes."$nodes"n.txt

#can be core/socket/none
procbind=$1

./haswell.run.generic.sh 1 $ppn $cps $spn $fn $nodes $nodefile $xmx $procbind $i $v $k $e -mms 500
