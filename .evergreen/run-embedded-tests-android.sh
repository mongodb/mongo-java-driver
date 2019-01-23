#!/bin/bash

#set -o xtrace   # Write all commands first to stderr
#set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

SDK_HOME=$PWD/.android
if [ ! -e  $SDK_HOME ]; then
    echo "Installing ANDROID SDK"
    DOWNLOAD_LOGS=${SDK_HOME}/download_logs
    mkdir -p $SDK_HOME
    mkdir -p $DOWNLOAD_LOGS
    (
        cd $SDK_HOME

        mkdir -p "$SDK_HOME/licenses"
        echo "24333f8a63b6825ea9c5514f83c2829b004d1fee" > "$SDK_HOME/licenses/android-sdk-license"

        export JAVA_HOME="/opt/java/jdk8"

        export ANDROID_HOME=${SDK_HOME}
        export ANDROID_SDK_ROOT=${SDK_HOME}
        export ANDROID_SDK_HOME=${SDK_HOME}

        SDK_PACKAGE=sdk-tools-linux-4333796.zip
        curl -O -s https://dl.google.com/android/repository/$SDK_PACKAGE
        unzip -o -qq $SDK_PACKAGE
        yes | $SDK_HOME/tools/bin/sdkmanager --channel=0 \
            "platforms;android-28"  \
            "emulator" \
            "patcher;v4" \
            "platform-tools"  \
            "build-tools;28.0.0" \
            "system-images;android-21;google_apis;x86_64"

        $SDK_HOME/tools/bin/sdkmanager --update
        PLATFORM_TOOLS=platform-tools-latest-linux.zip
        curl -O -s https://dl.google.com/android/repository/$PLATFORM_TOOLS
        unzip -o -qq $PLATFORM_TOOLS
    ) &> $DOWNLOAD_LOGS/download.log
fi

(
    export JAVA_HOME="/opt/java/jdk8"

    export ANDROID_HOME=${SDK_HOME}
    export ANDROID_SDK_ROOT=${SDK_HOME}
    export ANDROID_SDK_HOME=${SDK_HOME}

    echo no | $SDK_HOME/tools/bin/avdmanager create avd -n embeddedTest_x86_64 -c 1000M -k "system-images;android-21;google_apis;x86_64" -f
    $SDK_HOME/tools/emulator -avd embeddedTest_x86_64 -no-audio -no-window -no-snapshot -wipe-data -no-accel -gpu off &
    $SDK_HOME/platform-tools/adb wait-for-device

    # Belt and braces waiting for the device
    bootanim=""
    failcounter=0
    timeout_in_sec=360

    until [[ "$bootanim" =~ "stopped" ]]; do
      bootanim=`$SDK_HOME/platform-tools/adb -e shell getprop init.svc.bootanim 2>&1 &`
      if [[ "$bootanim" =~ "device not found" || "$bootanim" =~ "device offline"
        || "$bootanim" =~ "running" ]]; then
        let "failcounter += 1"
        if [[ "$failcounter" -gt timeout_in_sec ]]; then
          echo "Timeout ($timeout_in_sec seconds) reached; failed to start emulator"
          exit 1
        elif (( "$failcounter" % 10 )); then
           echo "Waiting for emulator to start"
        fi
      fi
      sleep 5
    done
    echo "Emulator is ready"
)

export JAVA_HOME="/opt/java/jdk9"
export ANDROID_HOME=${SDK_HOME}

function shutdownEmulators {
  retVal=$?
  $SDK_HOME/platform-tools/adb devices | grep emulator | cut -f1 | while read line; do $SDK_HOME/platform-tools/adb -s $line emu kill; done;
  exit $retVal
}

trap shutdownEmulators EXIT

echo "Running android tests"
./gradlew -version
./gradlew --stacktrace --info :driver-embedded-android:connectedAndroidTest -DANDROID_HOME=$SDK_HOME

