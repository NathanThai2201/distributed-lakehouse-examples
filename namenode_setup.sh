#!/bin/bash

# --- CONFIGURABLE VARIABLES ---
HDFS_PORT=9000

# Names
NAME_NODE="namenode"
DATA_NODE_1="datanode1"
DATA_NODE_2="datanode2"

# IPs
NAMENODE_IP="10.140.0.4"
DATANODE1_IP="10.140.0.2"
DATANODE2_IP="10.140.0.3"
# ------------------------------

set -e

echo "--- 1. Installing Java 11 (Adoptium) ---"
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | \
    gpg --dearmor | sudo tee /usr/share/keyrings/adoptium.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | \
    sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install temurin-11-jdk -y

JAVA_PATH=$(update-alternatives --list java | grep "temurin-11" | head -n 1)
JAVAC_PATH=$(update-alternatives --list javac | grep "temurin-11" | head -n 1)
sudo update-alternatives --set java "$JAVA_PATH"
sudo update-alternatives --set javac "$JAVAC_PATH"

echo "--- 2. System Networking (/etc/hosts) ---"
# Only add these if the names don't already exist in /etc/hosts
if ! grep -q "$NAME_NODE" /etc/hosts; then
sudo tee -a /etc/hosts <<EOF
$NAMENODE_IP $NAME_NODE
$DATANODE1_IP $DATA_NODE_1
$DATANODE2_IP $DATA_NODE_2
EOF
echo "Hosts file updated."
fi

echo "--- 3. Getting SSH and PDSH ---"
sudo apt-get install ssh pdsh -y

echo "--- 4. Installing Spark 3.5.1 ---"
sudo rm -rf /opt/spark
cd /tmp
wget -c https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
rm -f spark-3.5.1-bin-hadoop3.tgz

echo "--- 5. Installing Hadoop 3.3.6 ---"
rm -rf ~/hadoop
wget -c https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 ~/hadoop

export JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
export HADOOP_HOME=$HOME/hadoop
export SPARK_HOME=/opt/spark

echo "--- 6. Configuring Hadoop Distributed Files ---"
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/yarn-env.sh
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# core-site.xml
cat <<EOF > $HADOOP_HOME/etc/hadoop/core-site.xml
<configuration>
 <property>
  <name>fs.defaultFS</name>
  <value>hdfs://$NAME_NODE:$HDFS_PORT</value>
 </property>
</configuration>
EOF

# hdfs-site.xml
cat <<EOF > $HADOOP_HOME/etc/hadoop/hdfs-site.xml
<configuration>
 <property>
  <name>dfs.replication</name>
  <value>3</value>
 </property>
</configuration>
EOF

# mapred-site.xml
cat <<EOF > $HADOOP_HOME/etc/hadoop/mapred-site.xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

# yarn-site.xml
cat <<EOF > $HADOOP_HOME/etc/hadoop/yarn-site.xml
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>$NAME_NODE</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

echo "--- 7. Configuring Hadoop Workers ---"
cat <<EOF > $HADOOP_HOME/etc/hadoop/workers
$DATA_NODE_1
$DATA_NODE_2
EOF

echo "--- 8. Configuring Spark Environment ---"
sudo cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
sudo tee -a $SPARK_HOME/conf/spark-env.sh <<EOF
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_MASTER_HOST=localhost
export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=4
export SPARK_EXECUTOR_CORES=4
export JAVA_HOME=$JAVA_HOME
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
EOF

sudo cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
sudo tee -a $SPARK_HOME/conf/spark-defaults.conf <<EOF
spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME
spark.executorEnv.JAVA_HOME=$JAVA_HOME
spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3
spark.executorEnv.PYSPARK_PYTHON=python3
EOF

echo "--- 9. Updating ~/.bashrc ---"
VAR_BLOCK=$(cat <<EOF
# Big Data Environment Variables
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=\$HOME/hadoop
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF
)

if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    echo "$VAR_BLOCK" >> ~/.bashrc
    echo "Environment variables added to .bashrc"
fi

echo "--- 10. Installing Apache Airflow ---"
sudo apt install python3-pip python3-venv -y

# Setup Directory and Venv
mkdir -p ~/airflow
python3 -m venv ~/airflow/airflow-venv
source ~/airflow/airflow-venv/bin/activate
pip install --upgrade pip

# Constraints are key for Airflow stability
AIRFLOW_VERSION=2.9.0
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Initialize DB and Admin
export AIRFLOW_HOME=~/airflow
airflow db init

# Only create user if it doesn't exist (to avoid script errors on re-run)
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "Admin user already exists."

# Persist Airflow home in bashrc
if ! grep -q "AIRFLOW_HOME" ~/.bashrc; then
    echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc
    echo "alias start-airflow='source ~/airflow/airflow-venv/bin/activate && airflow standalone'" >> ~/.bashrc
fi

echo "--- 11. Installing MinIO ---"
# Binary Installation
wget -q -c https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/

wget -q -c https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# Create data directory
mkdir -p ~/minio-data

# Add Credentials to .bashrc so 'mc' works later
if ! grep -q "MINIO_ROOT_USER" ~/.bashrc; then
    cat <<EOF >> ~/.bashrc
# MinIO Config
export MINIO_ROOT_USER=admin
export MINIO_ROOT_PASSWORD=12345678
alias start-minio='minio server ~/minio-data --address ":9001" --console-address ":9002" &'
EOF
fi

echo "MinIO installed. Use 'start-minio' to launch."


echo "------------------------------------------------"
echo "SETUP COMPLETE!"
echo "Namenode: $NAME_NODE ($NAMENODE_IP) on Port $HDFS_PORT"
echo "------------------------------------------------"
exec bash --login