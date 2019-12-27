package tarzan.helpers.rdd;

import io.github.radkovo.rdf4j.builder.TargetModel;
import cz.vutbr.fit.ta.core.RDFConnector;
import cz.vutbr.fit.ta.core.ResourceFactory;
import cz.vutbr.fit.ta.halyard.RDFConnectorHalyard;
import cz.vutbr.fit.ta.ontology.Timeline;
import cz.vutbr.fit.ta.splaso.PlasoEntry;
import cz.vutbr.fit.ta.splaso.SparkPlasoSource;
import org.apache.spark.api.java.JavaRDD;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import scala.Tuple2;
import tarzan.helpers.py4j.PythonObjectsReflexion;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class HalyardRDD {

    public static final String MODULE_NAME = "plaso";
    public static final String PROFILE_ID = "plasotest";

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD                          a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName                        Halyard repository/HBase table
     * @param hbaseZookeeperQuorumOrConfigPath HBase Zookeeper quorum of HBase config path
     * @param hbaseZookeeperClientPort         the Zookeeper client port
     */
    public static void saveToHalyard(JavaRDD<Object> javaRDD, String tableName, String hbaseZookeeperQuorumOrConfigPath,
                                     Integer hbaseZookeeperClientPort) throws IOException {
        javaRDD.map(HalyardRDD::objectOfObjectArrayToObjectTuple2)
                .map(eventTuple -> HalyardRDD.mapToPlasoEntryFunction(eventTuple._1, eventTuple._2))
                .foreachPartition(plasoEntryIterator -> HalyardRDD.foreachPartitionFunction(plasoEntryIterator,
                        tableName, hbaseZookeeperQuorumOrConfigPath, hbaseZookeeperClientPort));
    }

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD    a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName  Halyard repository/HBase table
     * @param configPath HBase config path
     */
    public static void saveToHalyard(JavaRDD<Object> javaRDD, String tableName, String configPath) throws IOException {
        HalyardRDD.saveToHalyard(javaRDD, tableName, configPath, null);
    }

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD   a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName Halyard repository/HBase table
     */
    public static void saveToHalyard(JavaRDD<Object> javaRDD, String tableName) throws IOException {
        HalyardRDD.saveToHalyard(javaRDD, tableName, null, null);
    }

    protected static Tuple2<Object, Object> objectOfObjectArrayToObjectTuple2(Object object) {
        final Object[] objectsArray = PythonObjectsReflexion.objectToObjectArray(object);
        if ((objectsArray != null) && (objectsArray.length >= 2)) {
            return new Tuple2<Object, Object>(objectsArray[0], objectsArray[1]);
        } else {
            return null;
        }
    }

    protected static PlasoEntry mapToPlasoEntryFunction(Object event, Object eventData) {
        final Map<String, Object> eventStringObjectMap = PythonObjectsReflexion.objectToAttrValMap(event);
        final Map<String, Object> eventDataStringObjectMap = PythonObjectsReflexion.objectToAttrValMap(eventData);
        final Map<String, String> eventMap = PythonObjectsReflexion.stringObjectMapToStringStringMap(eventStringObjectMap);
        final Map<String, String> eventDataMap = PythonObjectsReflexion.stringObjectMapToStringStringMap(eventDataStringObjectMap);
        return new PlasoEntry(eventMap, eventDataMap);
    }

    protected static Resource getContext() {
        final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        final Date today = Calendar.getInstance().getTime();
        final String stamp = df.format(today);
        return ResourceFactory.createResourceIRI(MODULE_NAME, "context", stamp);
    }

    protected static RDFConnector createRDFConnector(String tableName, String hbaseZookeeperQuorumOrConfigPath, Integer hbaseZookeeperClientPort) throws IOException {
        if ((hbaseZookeeperQuorumOrConfigPath != null) && (hbaseZookeeperClientPort != null)) {
            return new RDFConnectorHalyard(hbaseZookeeperQuorumOrConfigPath, hbaseZookeeperClientPort, tableName);
        } else if (hbaseZookeeperQuorumOrConfigPath != null) {
            return new RDFConnectorHalyard(hbaseZookeeperQuorumOrConfigPath, tableName);
        } else {
            return new RDFConnectorHalyard(tableName);
        }
    }

    /**
     * Function usable in RDD.forEachPartition(...) to iterate through a list of Plaso's Entries and insert them into Halyard.
     * The RDFConnector must be created here, i.e., at particular workers (not at driver).
     *
     * @param plasoEntryIterator               an iterator through the Plase Entries
     * @param tableName                        Halyard repository/HBase table
     * @param hbaseZookeeperQuorumOrConfigPath HBase Zookeeper quorum of HBase config path
     * @param hbaseZookeeperClientPort         the Zookeeper client port
     * @throws IOException RDFConnector cannot be created
     */
    protected static void foreachPartitionFunction(Iterator<PlasoEntry> plasoEntryIterator,
                                                   String tableName, String hbaseZookeeperQuorumOrConfigPath,
                                                   Integer hbaseZookeeperClientPort) throws IOException {
        final RDFConnector rdfConnector = HalyardRDD.createRDFConnector(tableName, hbaseZookeeperQuorumOrConfigPath, hbaseZookeeperClientPort);
        final List<PlasoEntry> plasoEntriesList =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(plasoEntryIterator, Spliterator.ORDERED), false)
                        .collect(Collectors.toList());
        final SparkPlasoSource sparkPlasoSource = new SparkPlasoSource(PROFILE_ID, plasoEntriesList);
        final Timeline timeline = sparkPlasoSource.getTimeline();
        final TargetModel targetModel = new TargetModel(new LinkedHashModel());
        targetModel.add(timeline);
        rdfConnector.add(targetModel.getModel(), getContext());
        rdfConnector.close();
    }

}
