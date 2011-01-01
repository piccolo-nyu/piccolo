#!/bin/bash
H_HOME="/home/yavcular/hadoop-0.21.0"

rm "$H_HOME"/hadoop-piccolo-to-run.jar
rm -rf "$H_HOME"/yasemin-classpath/*

javac -classpath "$H_HOME"/hadoop-hdfs-0.21.0.jar:"$H_HOME"/hadoop-common-0.21.0.jar:"$H_HOME"/hadoop-mapred-0.21.0.jar:"$H_HOME"/hadoop-mapred-tools-0.21.0.jar:"$H_HOME"/lib/commons-cli-1.2.jar:"$H_HOME"/lib/commons-logging-1.1.1.jar:"$H_HOME"/lib/*.jar -d "$H_HOME"/yasemin-classpath/ "$H_HOME"/piccolo/edu/nyu/cs/piccolo/*.java "$H_HOME"/piccolo/edu/nyu/cs/piccolo/kernel/*.java "$H_HOME"/piccolo/edu/nyu/cs/piccolo/examples/*.java
#javac -classpath "$H_HOME":"$H_HOME"/lib  -d "$H_HOME"/yasemin-classpath/ "$H_HOME"/piccolo/edu/nyu/cs/piccolo/*.java "$H_HOME"/piccolo/edu/nyu/cs/piccolo/kernel/*.java "$H_HOME"/piccolo/edu/nyu/cs/piccolo/examples/*.java
jar -cvf hadoop-piccolo-to-run.jar -C "$H_HOME"/yasemin-classpath/ .

