package tarzan.helpers.rdd;

import cz.vutbr.fit.ta.core.RDFConnector;
import cz.vutbr.fit.ta.core.ResourceFactory;
import cz.vutbr.fit.ta.halyard.RDFConnectorHalyard;
import cz.vutbr.fit.ta.ontology.Timeline;
import cz.vutbr.fit.ta.splaso.PlasoEntry;
import cz.vutbr.fit.ta.splaso.PlasoJsonParser;
import cz.vutbr.fit.ta.splaso.SparkPlasoSource;
import io.github.radkovo.rdf4j.builder.TargetModel;
import org.apache.spark.api.java.JavaRDD;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class HalyardRDD {

    private static final String MODULE_NAME = "plaso";
    private static final String PROFILE_ID = "plasotest";
    private static final Charset JSON_ENCODING = StandardCharsets.US_ASCII;
    private static final PlasoJsonParser PLASO_PARSER = new PlasoJsonParser();

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
        javaRDD.map(jsonString -> PLASO_PARSER.parseSingleEntry(new ByteArrayInputStream(((String) jsonString).getBytes(JSON_ENCODING))))
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
