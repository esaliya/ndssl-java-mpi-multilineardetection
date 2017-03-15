#!/bin/bash
#for i in 1 2 3 4 6 8 12 24 
#for i in 12
for i in 24
do
  ./juliet.run.all.sh core $i 16
done

#./juliet.run.all.sh corepack 64 4
