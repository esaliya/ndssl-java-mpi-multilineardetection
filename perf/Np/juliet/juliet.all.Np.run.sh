#!/bin/bash
#for i in 1 2 4 8 16 32 
#for i in 12
for i in 1
do
  #./juliet.run.all.sh core $i 1 $1 16 true
  ./juliet.run.all.sh socket $i 15 $1 16 true
done

#./juliet.run.all.sh corepack 64 4
