#!/bin/bash
for i in 1 2 4 8 16 32 
#for i in 12
#for i in 6
do
  ./haswell.run.all.sh core $i 1
done

#./juliet.run.all.sh corepack 64 4
