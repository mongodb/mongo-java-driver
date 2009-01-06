#!/bin/bash

cd `dirname $0`

export ED_HOME=.
# TODO get rid of this?
export TZ=America/New_York

export CLASSPATH=.:build:conf:"$CLASSPATH"
for j in `find include \( -name webtest -type d \) -prune -o -type f -name '*.jar' -print`; do
    export CLASSPATH="$CLASSPATH":$j
done
export CLASSPATH="$CLASSPATH":/opt/java/lib/tools.jar

export java_lib_path="-Djava.library.path=include"
export headless="-Djava.awt.headless=true"
export jruby_home="-Djruby.home=$ED_HOME/include/ruby"

export standard_options="-enableassertions $java_lib_path $headless $jruby_home"

export memory_large="-Xmx1000m -XX:MaxDirectMemorySize=600M"
export memory_small="-Xmx200m  -XX:MaxDirectMemorySize=200M"

export java_memory_large="java $standard_options $memory_large"
export java_memory_small="java $standard_options $memory_small"
