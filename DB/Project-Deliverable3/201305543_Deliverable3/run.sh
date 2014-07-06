#!/bin/bash

filepath=`readlink -f $1`
cd 201305543_src
javac -cp gsp.jar:. *.java
java -cp gsp.jar:. ParseQuery $filepath
