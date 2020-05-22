# pyspark-plaso-java-helpers

(c) 2019-2020 Marek Rychly (rychly@fit.vutbr.cz)

Parts of PySpark Plaso that must be implemented in Java to help with the RDD processing.

## Usage

Copy the `*.jar` files created by the build scripts below into the `/app/lib` directory
of the Spark application Docker image running the PySpark Plaso, i.e.,
`registry.gitlab.com/rychly-edu/docker/docker-spark-app:spark2.4-hadoop3.1`.

See also deployment scripts in the PySpark Plaso project.

## Build

~~~
./build-deps.sh
./gradlew build
ls -l ./build/libs/*.jar
~~~

## Acknowledgements

*This work was supported by the Ministry of the Interior of the Czech Republic as a part of the project Integrated platform for analysis of digital data from security incidents VI20172020062.*
