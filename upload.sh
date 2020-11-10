#!/bin/bash

set -e

scp target/universal/akka-hdfs-0.1.0-SNAPSHOT.zip apps:~

ssh apps "unzip -o akka-hdfs-0.1.0-SNAPSHOT.zip"