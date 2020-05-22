#!/bin/sh

DIRNAME=$(dirname ${0})

cd ${DIRNAME}
. ./gradlew --build-cache build $@
