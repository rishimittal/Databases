#!/bin/bash

cd 201305543_src
javac -cp gsp.jar:. *.java
java -cp gsp.jar:. ParseQuery $1 $2 
