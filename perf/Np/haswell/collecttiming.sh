#!/bin/bash
fname=$1
nameonly=${fname##*/}
nameonly=${nameonly%.*}
nameonly=${nameonly%.*}
val=`grep "End of Loop: 0" $fname | cut -d ' ' -f8`
printf "$nameonly\t$val\n" >> allouttiming.txt
