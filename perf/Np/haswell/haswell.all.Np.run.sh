#!/bin/bash
#for i in 1 2 4 8 16 32 
#for i in 12
for i in 1
do
  ./haswell.run.all.sh socket $i 16 $1 16 true
done
