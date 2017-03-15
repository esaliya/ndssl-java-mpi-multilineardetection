#!/bin/bash
u=`ulimit -u`
echo  "Rank: " $OMPI_COMM_WORLD_RANK " " $u
