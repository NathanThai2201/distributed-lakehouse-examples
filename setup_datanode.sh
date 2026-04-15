#!/bin/bash
set -e

echo "=== DATANODE SETUP ==="

read -r -p "Namenode PRIVATE IP: " NN_PRIVATE_IP
read -r -p "Datanode1 PRIVATE IP: " DN1_PRIVATE_IP
read -r -p "Datanode2 PRIVATE IP: " DN2_PRIVATE_IP
read -r -p "Datanode username: " DN_USER

if [ "$(whoami)" != "$DN_USER" ]; then
  echo "ERROR: Run as user '$DN_USER' (now: $(whoami))."
  exit 1
fi

BASE_HOME="/home/$DN_USER"
HADOOP_HOME="$BASE_HOME/hadoop"
SPARK_HOME="/opt/spark"
JAVA_HOME="/usr/lib/jvm/temurin-11-jdk-amd64"

echo "===================="
echo "STEP: CHECK HADOOP COPIED FROM NAMENODE"
echo "===================="
if [ ! -x "$HADOOP_HOME/bin/hdfs" ]; then
  echo "ERROR: $HADOOP_HOME missing (no hdfs). Run setup_namenode.sh first so it rsyncs Hadoop here."
  exit 1
else
  echo "  - Hadoop already present"
fi

echo "===================="
echo "STEP: CHECK SPARK FROM NAMENODE (/opt/spark)"
echo "===================="
if [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "  - Spark already present"
else
  echo "  - WARNING: $SPARK_HOME/bin/spark-submit missing — run namenode push step or install Spark; YARN/Spark jobs need this"
fi

echo "[Step] Configure apt mirror (Kakao)"
if grep -qE "mirror\\.kakao\\.com/ubuntu" /etc/apt/sources.list; then
  echo "  - Already configured"
else
  sudo sed -i 's|http://archive.ubuntu.com/ubuntu|http://mirror.kakao.com/ubuntu|g' /etc/apt/sources.list
  sudo sed -i 's|http://security.ubuntu.com/ubuntu|http://mirror.kakao.com/ubuntu|g' /etc/apt/sources.list
  echo "  - Configured"
fi

echo "===================="
echo "STEP: INSTALL SYSTEM BASE PACKAGES"
echo "===================="
BASE_PKGS="wget gpg ssh pdsh python3-venv python3-pip curl tar rsync unzip"
MISSING_PKGS=""
for p in $BASE_PKGS; do
  if dpkg -s "$p" >/dev/null 2>&1; then
    :
  else
    MISSING_PKGS="$MISSING_PKGS $p"
  fi
done
if [ -n "$MISSING_PKGS" ]; then
  sudo apt update
  sudo apt install -y $MISSING_PKGS
  echo "  - Installed missing packages:$MISSING_PKGS"
else
  echo "  - Already installed"
fi

echo "===================="
echo "STEP: INSTALL JAVA 11 (TEMURIN 11)"
echo "===================="
if [ -d "$JAVA_HOME" ]; then
  echo "  - Already installed"
else
  if [ ! -f /usr/share/keyrings/adoptium.gpg ]; then
    wget -4 -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor -o /usr/share/keyrings/adoptium.gpg
  fi
  . /etc/os-release
  ADOPT_CODENAME="${VERSION_CODENAME:-bookworm}"
  echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb ${ADOPT_CODENAME} main" | sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
  sudo apt update
  sudo apt install -y temurin-11-jdk
  echo "  - Installed"
fi

echo "===================="
echo "STEP: CONFIGURE /etc/hosts"
echo "===================="
# Escape dots so IPs match literally in regex
_nn_re=$(printf '%s' "$NN_PRIVATE_IP" | sed 's/\./\\./g')
_dn1_re=$(printf '%s' "$DN1_PRIVATE_IP" | sed 's/\./\\./g')
_dn2_re=$(printf '%s' "$DN2_PRIVATE_IP" | sed 's/\./\\./g')
if grep -qE "^${_nn_re}[[:space:]]+namenode([[:space:]]|$)" /etc/hosts \
  && grep -qE "^${_dn1_re}[[:space:]]+datanode1([[:space:]]|$)" /etc/hosts \
  && grep -qE "^${_dn2_re}[[:space:]]+datanode2([[:space:]]|$)" /etc/hosts; then
  echo "  - Already configured (namenode + datanode1 + datanode2 with expected IPs)"
else
  sudo bash -c "cat >> /etc/hosts" <<EOT
$NN_PRIVATE_IP namenode
$DN1_PRIVATE_IP datanode1
$DN2_PRIVATE_IP datanode2
EOT
  echo "  - Configured (appended cluster lines)"
fi

echo "===================="
echo "STEP: CONFIGURE JAVA_HOME IN hadoop-env.sh"
echo "===================="
HADOOP_ENV="$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
if grep -qE "^export JAVA_HOME=$JAVA_HOME" "$HADOOP_ENV"; then
  echo "  - Already configured (exact JAVA_HOME already set)"
elif grep -qE '^# export JAVA_HOME=' "$HADOOP_ENV"; then
  sed -i "s|^# export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" "$HADOOP_ENV"
  echo "  - Updated from commented line (# export JAVA_HOME=...)"
elif grep -qE '^export JAVA_HOME=' "$HADOOP_ENV"; then
  sed -i "s|^export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME|" "$HADOOP_ENV"
  echo "  - Updated existing export JAVA_HOME value"
else
  echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_ENV"
  echo "  - Added new export JAVA_HOME line"
fi

echo "===================="
echo "STEP: CONFIGURE SHELL ENVIRONMENT"
echo "===================="
if grep -qE "HADOOP_HOME" ~/.bashrc; then
  echo "  - Already configured"
else
  cat <<EOT >> ~/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOT
  echo "  - Configured"
fi

export JAVA_HOME HADOOP_HOME SPARK_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
echo "  - Environment exported for current script run"

echo "===================="
echo "STEP: CREATE DATANODE DIRECTORY"
echo "===================="
if [ -d "$BASE_HOME/hadoopdata/datanode" ]; then
  chmod -R 700 "$BASE_HOME/hadoopdata"
  echo "  - Already exists (permissions refreshed)"
else
  mkdir -p "$BASE_HOME/hadoopdata/datanode"
  chmod -R 700 "$BASE_HOME/hadoopdata"
  echo "  - Created"
fi

echo "=== DATANODE SETUP DONE ==="
echo "From namenode: start-dfs.sh && start-yarn.sh"
