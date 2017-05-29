#!/bin/bash

# example of using arguments to a script
echo "My first name is $1"
echo "My surname is $2"
echo "Total number of arguments is $#" 
hadoop fs -rmr -skipTrash $2
yarn jar /home/edureka/SAIWS/batch030417/JobJars/CPDemo.jar com.laboros.job.CPJob $1 $2
