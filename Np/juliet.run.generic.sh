#!/bin/bash

cp=$HOME/.m2/repository/org/saliya/ndssl/multilineardetection/1.0-SNAPSHOT/multilineardetection-1.0-SNAPSHOT-jar-with-dependencies.jar

wd=`pwd`
x='x'

tpp=$1
ppn=$2

cps=$3
spn=$4
cpn=$(($cps*$spn))

name=$5
nodes=$6
nodefile=$7
xmx=$8
procbind=$9

i=${10}
v=${11}
k=${12}
e=${13}

pe=$(($cpn/$ppn))

pat=$tpp$x$ppn$x$nodes

opts="-XX:+UseSerialGC -Xms256m -Xmx$xmx"

echo "Running $pat on `date`" >> status.txt
if [ $procbind = "core" ]; then
  $BUILD/bin/mpirun --mca orte_base_help_aggregate 0 --verbose --hostfile $nodefile -mca btl_openib_ib_min_rnr_timer 25 -mca btl_openib_ib_timeout 20  --mca btl ^tcp --report-bindings --map-by ppr:$ppn:node:PE=$pe  --bind-to core --rank-by core  -np $(($nodes*$ppn)) java $opts -cp $cp org.saliya.ndssl.multilinearscan.mpi.Program -v $v -k $k -i $i -e $e -nc $nodes 2>&1 | tee $name.v$v.k$k.e$e.$pat.out.txt
elif [ $procbind = "corepack" ]; then
  np=$2
  $BUILD/bin/mpirun --mca orte_base_help_aggregate 0 --verbose --hostfile $nodefile -mca btl_openib_ib_min_rnr_timer 25 -mca btl_openib_ib_timeout 20  --mca btl ^tcp --report-bindings --bind-to core --rank-by core  -np $np java $opts -cp $cp org.saliya.ndssl.multilinearscan.mpi.Program -v $v -k $k -i $i -e $e -nc $nodes 2>&1 | tee $name.v$v.k$k.e$e.$pat.out.txt
else
  echo $BUILD/bin/mpirun --hostfile $7 --mca btl ^tcp --report-bindings --map-by ppr:$ppn:node  --bind-to none --rank-by core  -np $(($nodes*$ppn)) java $opts  $MDS_OPS -cp $cp edu.indiana.soic.spidal.damds.ProgramLRT -c ../config.properties.$data -n $nodes -t $tpp -mmaps $mmaps -mmapdir $mmapdir -bind $explicitbind -cps $cps 2>&1 | tee $data.$pat.$xmx.$memmultype.$4.$3.comm.$commpat.out.txt
fi
echo "Finished $pat on `date`" >> status.txt

