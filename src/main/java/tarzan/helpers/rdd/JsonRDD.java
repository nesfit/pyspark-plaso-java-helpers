package tarzan.helpers.rdd;

import org.apache.spark.api.java.JavaRDD;
import tarzan.helpers.py4j.PythonObjectsByGson;

import java.util.List;

/**
 * A class for converting Java RDDs to JSON documents by provided static methods. Usage in PySpark:
 * <pre>
 *     from pyspark.mllib.common import _py2java, _java2py
 *     input_python_rdd = sc.parallelize(range(1, 10))
 *     input_java_rdd = _py2java(sc, input_python_rdd)
 *     json_rdd_class = sc._gateway.jvm.tarzan.helpers.rdd.JsonRDD
 *     print json_rdd_class.collectToJsonCollection(input_java_rdd)
 *     output_java_rdd = json_rdd_class.mapToJsonStrings(input_java_rdd)
 *     output_python_rdd = _java2py(sc, output_java_rdd)
 *     print output_python_rdd.collect()
 * </pre>
 * See also https://stackoverflow.com/a/34412182/5265908 and https://stackoverflow.com/a/39459886/5265908
 */
public class JsonRDD {

    /**
     * Collects and returns a JSON collection of all collected object in the RDD.
     *
     * @param javaRDD a JavaRDD object
     * @return the JSON collection of the collected data (one item per JSON document in the collection)
     */
    public static String collectToJsonCollection(JavaRDD<Object> javaRDD) {
        StringBuilder stringBuilder = new StringBuilder();
        List<Object> objects = javaRDD.collect();
        for (Object object : objects) {
            stringBuilder.append(PythonObjectsByGson.objectToJsonString(object)).append("\n");
        }
        return stringBuilder.toString();
    }

    /**
     * Transform input JavaRDD by mapping its items to their JSON string documents.
     *
     * @param javaRDD a JavaRDD object to transform
     * @return the resulting JavaRDD object of JSON string documents
     */
    public static JavaRDD<String> mapToJsonStrings(JavaRDD<Object> javaRDD) {
        return javaRDD.map(PythonObjectsByGson::objectToJsonString);
    }
}
