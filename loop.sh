#! /bin/sh

for (( seg=0; seg<5; seg++ ))
do
   bash batch-write.sh 5 $seg 
done
