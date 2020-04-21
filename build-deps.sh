#!/bin/sh

set -e

OLD_PWD=$(pwd)
SCRIPTNAME=$(readlink -f $0) DIRNAME=$(dirname ${SCRIPTNAME})/build

mkdir -p ${DIRNAME}/timeline-analyzer-builder
cd ${DIRNAME}/timeline-analyzer-builder

## https://github.com/nesfit/timeline-analyzer/blob/master/misc/clone.sh

for I in \
	"https://github.com/nesfit/timeline-analyzer.git|hbase2" \
	"https://github.com/radkovo/rdf4j-vocab-builder.git" \
	"https://github.com/radkovo/rdf4j-class-builder.git" \
;
#	"https://github.com/radkovo/sqljet.git" \
do
	REPO=${I%|*}
	BRANCH=$(echo ${I} | cut -s -d '|' -f 2)
	DIR=$(basename ${REPO} .git)
	echo "# repo: ${REPO}" 1>&2
	echo "# branch: ${BRANCH}" 1>&2
	echo "# dir: ${DIR}" 1>&2
	if [ -d ${DIR} ]; then
		cd ${DIR}
		git pull --ff-only --depth 1
		[ -n "${BRANCH}" ] && git checkout ${BRANCH}
		cd -
	else
		[ -n "${BRANCH}" ] && BRANCH_ARG="--branch ${BRANCH}" || unset BRANCH_ARG
		git clone --depth 1 ${BRANCH_ARG} ${REPO}
	fi
done

## https://github.com/nesfit/timeline-analyzer/blob/master/misc/install.sh

# Halyard JAR () and sqljet (required by timeline-analyzer-local)
cd ./timeline-analyzer/misc
source ./install.sh
cd -

# sqljet (required by timeline-analyzer-local)
#cd ./sqljet
#mvn install -DskipTests
#cd -

# rdf4j-vocab-builder
cd ./rdf4j-vocab-builder
mvn install -DskipTests
cd -

# rdf4j-class-builder
cd ./rdf4j-class-builder
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
