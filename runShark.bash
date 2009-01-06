#!/bin/bash

. `dirname $0`/common.bash
ant && $java_memory_small -XrunShark "$@"
