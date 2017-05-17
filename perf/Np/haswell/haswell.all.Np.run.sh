#!/bin/bash
#for i in 1 2 4 8 16 32 
#for i in 32
#for i in 1
#do
  #./haswell.run.all.sh socket $i 1 $1 1 true
  #./haswell.run.all.sh socket 1 2 $1 $i true
  #./haswell.run.all.sh socket 1 4 $1 $i true
  #./haswell.run.all.sh socket 1 8 $1 $i true
  #./haswell.run.all.sh socket 1 16 $1 $i true
#done

#./haswell.run.all.sh socket 32 2 $1 1 true
./haswell.run.all.sh socket 32 4 $1 1 true
#./haswell.run.all.sh socket 32 8 $1 1 true
#./haswell.run.all.sh socket 32 16 $1 1 true
