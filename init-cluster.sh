#!/bin/bash

mkdir $LOGDIR

source $AWSKEYFILE

echo "Launching cluster..."
mesos-ec2 \
    --slaves=$NUMSLAVES \
    --key-pair=$KEYPAIR \
    --identity-file=$KEYPAIRFILE \
    --instance-type=$INSTANCETYPE \
    --zone=$ZONE launch $CLUSTERNAME

MASTER=$(mesos-ec2 -k $KEYPAIR -i $KEYPAIRFILE get-master $CLUSTERNAME | tail -n 1)
MESOSMASTER=$(curl http://$MASTER:8080 2>/dev/null | perl -ne 'print $1 if /^PID: (.*)<br \/>/')
echo "Master public hostname: $MASTER"
echo "Master Mesos PID: $MESOSMASTER"

echo "Configuring cluster: Updating Spark from Git..."
ssh -i $KEYPAIRFILE root@$MASTER \
    "cd /root/spark; \
      git pull https://github.com/mesos/spark.git master; \
      make clean"
echo \
    "export MESOS_HOME=/root/mesos
      export SCALA_HOME=/root/scala-2.8.0.final
      export SPARK_HOME=/root/spark
      export SPARK_CLASSPATH=/root/pregel-spark/build/pregel-spark.jar
      export SPARK_MEM=$MEMORY
      export SPARK_JAVA_OPTS=\"-Dspark.cache.class=spark.BoundedMemoryCache -Dspark.boundedMemoryCache.memoryFraction=0.5\"" > /tmp/$CLUSTERNAME-spark-env.sh
scp -i $KEYPAIRFILE /tmp/$CLUSTERNAME-spark-env.sh root@$MASTER:/root/spark/conf/spark-env.sh

echo "Configuring cluster: Setting up pregel-spark..."
ssh -i $KEYPAIRFILE root@$MASTER \
    "apt-get -y install mercurial; \
      cd /root; \
      hg clone http://hg.ankurdave.com/pregel-spark; \
      cd pregel-spark; \
      source ~/.profile; \
      source /root/spark/conf/spark-env.sh; \
      make"

echo "Configuring cluster: Copying modified files to slaves..."
ssh -i $KEYPAIRFILE root@$MASTER \
    "cd /root; \
      mesos-ec2/copy-dir spark; \
      mesos-ec2/copy-dir pregel-spark"

export INPUTFILE=hdfs://$MASTER:9000/wex.dat
