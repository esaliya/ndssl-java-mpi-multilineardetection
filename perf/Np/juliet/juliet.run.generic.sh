#!/bin/bash

cp=$HOME/.m2/repository/org/saliya/ndssl/multilineardetection/1.0-SNAPSHOT/multilineardetection-1.0-SNAPSHOT-jar-with-dependencies.jar:$HOME/sali/software/slf4j-1.7.21/slf4j-api-1.7.21.jar:$HOME/sali/software/slf4j-1.7.21/slf4j-nop-1.7.21.jar

wd=`pwd`
x='x'

tpp=$1
ppn=$2

cps=$3
spn=$4
cpn=$(($cps*$spn))

#procs per socket
if [ $ppn -eq 1 ]
then
  pps=$ppn
else
  pps=$(($ppn/$spn))
fi

name=$5
nodes=$6
nodefile=$7
xmx=$8
procbind=$9

i=${10}
v=${11}

mms=${12}
parts=${13}
partMethod=${14}
bindThreads=${15}

#pe=$(($cpn/$ppn))
pe=$(($cps/$pps))

pat=$tpp$x$ppn$x$nodes

opts="-XX:+UseSerialGC -Xms256m -Xmx$xmx"

echo "Running $pat on `date`" >> status.txt
if [ $procbind = "core" ]; then
  # same as socket
  $BUILD/bin/mpirun --mca btl ^tcp --mca mtl ^psm --hostfile $nodefile --report-bindings --map-by ppr:$ppn:socket:PE=$pe  --bind-to core --rank-by core  -np $(($nodes*$ppn)) java $opts -cp $cp org.saliya.ndssl.multilinearscan.mpi.Program -v $v -i $i -nc $nodes -mms $mms -p $parts -tc $tpp -cps $cps -bind $bindThreads 2>&1 | tee $name.v$v.k$k.e$e.$pat.$partMethod.out.txt
elif [ $procbind = "socket" ]; then
  $BUILD/bin/mpirun --mca btl ^tcp --mca mtl ^psm --hostfile $nodefile --report-bindings --map-by ppr:$ppn:socket:PE=$pe  --bind-to core --rank-by core  -np $(($nodes*$ppn)) java $opts -cp $cp org.saliya.ndssl.multilinearscan.mpi.Program -v $v -i $i -nc $nodes -mms $mms -p $parts -tc $tpp -cps $cps -bind $bindThreads 2>&1 | tee $name.v$v.k$k.e$e.$pat.$partMethod.out.txt
else
  echo $BUILD/bin/mpirun --hostfile $7 --mca btl ^tcp --report-bindings --map-by ppr:$ppn:node  --bind-to none --rank-by core  -np $(($nodes*$ppn)) java $opts  $MDS_OPS -cp $cp edu.indiana.soic.spidal.damds.ProgramLRT -c ../config.properties.$data -n $nodes -t $tpp -mmaps $mmaps -mmapdir $mmapdir -bind $explicitbind -cps $cps 2>&1 | tee $data.$pat.$xmx.$memmultype.$4.$3.comm.$commpat.out.txt
fi
echo "Finished $pat on `date`" >> status.txt

