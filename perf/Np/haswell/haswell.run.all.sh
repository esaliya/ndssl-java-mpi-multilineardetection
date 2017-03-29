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

#dir=random-er

#100k
#fn=0100000
#v=80991

#1mil
#fn=1000000
#v=809165

#10mil
fn=10000000
v=10000000

dir=/localscratch/esaliya/sali/projects/vt/giraph/random-er

i=$dir/"$fn".txt
k=6
e=0.1

nodes=$3
ppn=$2
tpp=$5
bind=$6
xmx=100g

totalPar=$(($nodes*$ppn*$tpp))
partMethod=$4
if [ "$partMethod" == 'simple' ] 
 then
  #make a non existent file name
  partsFile=$dir/"$fn".txt.simple.$totalPar
else
  partsFile=$dir/"$fn".txt.part.$totalPar
fi

cps=16
spn=2
nodefile=nodes."$nodes"n.txt

#can be core/socket/none
procbind=$1

./haswell.run.generic.sh $tpp $ppn $cps $spn $fn $nodes $nodefile $xmx $procbind $i $v $k $e 500 $partsFile $partMethod $bind
