#!/bin/sh

jps | grep LockProcess | awk '{print $1}' | xargs kill -9
