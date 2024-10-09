# graalvm-native-image-app

## About
This is an example of a native application that uses the driver and is built using
[GraalVM native image](https://www.graalvm.org/latest/reference-manual/native-image/).

## Contributor Guide

This guide assumes you are using a shell capable of running [Bash](https://www.gnu.org/software/bash/) scripts.

### Prepare the development environment

#### Install GraalVM

[GraalVM for JDK 21 Community](https://github.com/graalvm/graalvm-ce-builds/releases/tag/jdk-21.0.2) is required
in addition to the JDK you are using for running [Gradle](https://gradle.org/) when building the driver.
Note that GraalVM for JDK 21 Community is [available](https://sdkman.io/jdks#graalce) via [SDKMAN!](https://sdkman.io/).

##### Explanation of the requirement

* GraalVM Community is the only distribution of GraalVM for which it is possible to
  specify a Gradle toolchain specification that matches only GraalVM
  and does not match any other JDK.
* GraalVM for Java SE 21 is required because it is the latest released version at the moment,
  and not supporting the build for multiple, especially older versions, simplifies things.
  Releases of JDKs for Java SE 21 having a long-term support from most vendors
  also makes this version more attractive.  

#### Configure environment variables pointing to JDKs.

Assuming that the JDK you are using for running Gradle is for Java SE 17, export the following variables
(your values may differ):

```bash
export JDK17=$(realpath ~/".sdkman/candidates/java/17.0.10-librca/")
export JDK21_GRAALVM=$(realpath ~/".sdkman/candidates/java/21.0.2-graalce/")
```

##### Informing Gradle on JDK locations it does not know about

If `JDK21_GRAALVM` points to a
[location the Gradle auto-detection mechanism is not aware of](https://docs.gradle.org/current/userguide/toolchains.html#sec:auto_detection),
you need to inform Gradle about that location as specified in https://docs.gradle.org/current/userguide/toolchains.html#sec:custom_loc.

### Build-related commands

Assuming that your MongoDB deployment is accessible at `mongodb://localhost:27017`,
run from the driver project root directory:

| &#x23; | Command                                                                                                                                                                                                                          | Description                                                                                                                                   |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| 0      | `env JAVA_HOME="${JDK17}" ./gradlew -PjavaVersion=21 :graalvm-native-image-app:nativeCompile`                                                                                                                                    | Build the application relying on the reachability metadata stored in `graalvm-native-image-app/src/main/resources/META-INF/native-image`.     |
| 1      | `env JAVA_HOME="${JDK17}" ./gradlew clean && env JAVA_HOME=${JDK21_GRAALVM} ./gradlew -PjavaVersion=21 -Pagent :graalvm-native-image-app:run && env JAVA_HOME=${JDK21_GRAALVM} ./gradlew :graalvm-native-image-app:metadataCopy` | Collect the reachability metadata and update the files storing it. Do this before building the application only if building fails otherwise.  |
| 2      | `./graalvm-native-image-app/build/native/nativeCompile/NativeImageApp`                                                                                                                                                           | Run the application that has been built.                                                                                                      |
| 3      | `env JAVA_HOME="${JDK17}" ./gradlew -PjavaVersion=21 :graalvm-native-image-app:nativeRun`                                                                                                                                        | Run the application using Gradle, build it if necessary relying on the stored reachability metadata.                                          |

#### Specifying a custom connection string

If your MongoDB deployment is not accessible at `mongodb://localhost:27017`,
or you want to use a custom connection string,
you can specify the connection string used by the `:graalvm-native-image-app:run`, `:graalvm-native-image-app:nativeRun`
Gradle tasks, as well as by the built native application by passing the CLI argument
`-Dorg.mongodb.test.uri="<your connection string>"` to `gradlew` or `NativeImageApp` respectively:

For gradle to include this project in the build you *must* include a project flag: `-PincludeGraalvm`

```bash
./gradlew ... -PincludeGraalvm -Dorg.mongodb.test.uri="<your connection string>"
```

```bash
./graalvm-native-image-app/build/native/nativeCompile/NativeImageApp -PincludeGraalvm -Dorg.mongodb.test.uri="<your connection string>"
```
