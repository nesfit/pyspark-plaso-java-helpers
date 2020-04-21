#!/bin/sh

set -e

OLD_PWD=$(pwd)
SCRIPTNAME=$(readlink -f $0)
DEPDIR=$(dirname ${SCRIPTNAME})/deps
LIBDIR=$(dirname ${SCRIPTNAME})/build/libs

## https://github.com/nesfit/timeline-analyzer/blob/master/misc/install.sh

# Halyard JAR () and sqljet (required by timeline-analyzer-local)
cd ${DEPDIR}/timeline-analyzer/misc
source ./install.sh
cd -

# sqljet (required by timeline-analyzer-local)
#cd ${DEPDIR}/sqljet
#mvn install -DskipTests
#cd -

# rdf4j-vocab-builder
cd ${DEPDIR}/rdf4j-vocab-builder
mvn install -DskipTests
cd -

# rdf4j-class-builder
cd ${DEPDIR}/rdf4j-class-builder
mvn install -DskipTests
cd -

## timeline-spark without hadoop libs
cd ${DEPDIR}/timeline-analyzer/timeline-spark
sed -i -e 's|<!-- \(<exclusion>\)|\1|g' -e 's|\(</exclusion>\) -->|\1|g' pom.xml
cd -

## timeline-analyzer including timeline-spark

cd ${DEPDIR}/timeline-analyzer
mvn package
cd -

mkdir -p ${LIBDIR}
cp -v ${DEPDIR}/timeline-analyzer/timeline-spark/target/TA.jar ${LIBDIR}/timeline-analyzer-spark.jar

##

cd ${OLD_PWD}
