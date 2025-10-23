#
# Update the parameters below to match your environment
#
export DIR="$(pwd)"
export JDK_HOME=/usr/java/jdk
export JAVA_HOME=/usr/java/jdk
export CLASSPATH=${DIR}/target:.

date
echo "Building SPL db..."
cd ${DIR}
${JAVA_HOME}/bin/java -jar ./target/pvlens-spl-db-0.0.1-SNAPSHOT-jar-with-dependencies.jar > pvlens-spl-db-build.log
date
echo "Complete"
