#!/bin/sh

set -e

OLD_PWD=$(pwd)
SCRIPTNAME=$(readlink -f $0) DIRNAME=$(dirname ${SCRIPTNAME})/build

mkdir -p ${DIRNAME}/timeline-analyzer-builder
cd ${DIRNAME}/timeline-analyzer-builder

## https://github.com/nesfit/timeline-analyzer/blob/master/misc/clone.sh

for I in \
	https://github.com/nesfit/timeline-analyzer.git \
	https://github.com/radkovo/sqljet.git \
	https://github.com/radkovo/rdf4j-vocab-builder.git \
	https://github.com/radkovo/rdf4j-class-builder.git \
; do
	DIR=$(basename $I .git)
	if [ -d ${DIR} ]; then
		cd ${DIR}
		git pull --ff-only
		cd -
	else
		git clone --depth 1 $I
	fi
done

## https://github.com/nesfit/timeline-analyzer/blob/master/misc/install.sh

# Halyard JAR
cd ./timeline-analyzer
mvn install:install-file \
   -Dfile=./timeline-storage-halyard/lib/halyard-common-1.3.jar \
   -DgroupId=com.msd.gin.halyard.common \
   -DartifactId=halyard-common \
   -Dversion=1.3 \
   -Dpackaging=jar \
   -DgeneratePom=true
cd -

# rdf4j-vocab-builder
cd ./rdf4j-vocab-builder
mvn install -DskipTests
cd -

# rdf4j-class-builder
cd ./rdf4j-class-builder
mvn install -DskipTests
cd -

# sqljet (required by timeline-analyzer-local)
cd ./sqljet
mvn install -DskipTests
cd -

## timeline-spark without hadoop libs
cd ./timeline-analyzer/timeline-spark
sed -i -e 's|<!-- \(<exclusion>\)|\1|g' -e 's|\(</exclusion>\) -->|\1|g' pom.xml
cd -

## timeline-analyzer including timeline-spark

cd ./timeline-analyzer
mvn package

mkdir -p ${DIRNAME}/libs
cp -v ./timeline-spark/target/TA.jar ${DIRNAME}/libs/timeline-analyzer-spark.jar

##

cd ${OLD_PWD}
