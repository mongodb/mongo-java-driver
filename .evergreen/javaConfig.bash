# Java configurations for evergreen
# Defaults to Java 11

export JDK8="/opt/java/jdk8"
export JDK11="/opt/java/jdk11"
export JDK17="/opt/java/jdk17"
export JAVA_HOME=$JDK11

export JAVA_VERSION=${JDK:-11}

echo "Java Configs:"
echo "Java Home: ${JAVA_HOME}"
echo "Java test version: ${JAVA_VERSION}"
