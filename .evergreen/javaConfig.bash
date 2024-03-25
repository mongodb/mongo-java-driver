# Java configurations for evergreen

export JDK8="/opt/java/jdk8"
export JDK11="/opt/java/jdk11"
export JDK17="/opt/java/jdk17"
export JDK21="/opt/java/jdk21"
# note that `JDK21_GRAALVM` is used in `run-graalvm-native-image-app.sh`
# by dynamically constructing the variable name
export JDK21_GRAALVM="/opt/java/jdk21-graalce"

if [ -d "$JDK17" ]; then
  export JAVA_HOME=$JDK17
fi

export JAVA_VERSION=${JAVA_VERSION:-17}

echo "Java Configs:"
echo "Java Home: ${JAVA_HOME}"
echo "Java test version: ${JAVA_VERSION}"
