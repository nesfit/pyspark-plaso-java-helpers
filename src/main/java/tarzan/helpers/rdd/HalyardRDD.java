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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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

    protected static ByteArrayInputStream objectToByteArrayInputStream(Object object) throws IOException {
        if (object instanceof String) {
            // it is a String object
            final String stringObject = (String) object;
            final byte[] byteArray = stringObject.getBytes(JSON_ENCODING);
            return new ByteArrayInputStream(byteArray);
        } else {
            // it is a cPickle object (see a debug file)
            try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(object);
                objectOutputStream.flush();
                final byte[] byteArray = byteArrayOutputStream.toByteArray();
                /* DEBUG *
                try (FileOutputStream fileOutputStream
                             = new FileOutputStream("/tmp/objectToByteArrayInputStream_" + object.toString())) {
                    fileOutputStream.write(byteArray);
                }
                /* */
                return new ByteArrayInputStream(byteArray);
            }
        }
    }

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD                          a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName                        Halyard repository/HBase table
     * @param hbaseZookeeperQuorumOrConfigPath HBase Zookeeper quorum of HBase config path
     * @param hbaseZookeeperClientPort         the Zookeeper client port
     * @return how long the processing took in nano-seconds
     */
    public static long saveToHalyard(JavaRDD<Object> javaRDD, String tableName, String hbaseZookeeperQuorumOrConfigPath,
                                     Integer hbaseZookeeperClientPort) throws IOException {
        long startTime = System.nanoTime();
        javaRDD.map(jsonString -> PLASO_PARSER.parseSingleEntry(HalyardRDD.objectToByteArrayInputStream(jsonString)))
                .foreachPartition(plasoEntryIterator -> HalyardRDD.foreachPartitionFunction(plasoEntryIterator,
                        tableName, hbaseZookeeperQuorumOrConfigPath, hbaseZookeeperClientPort));
        long estimatedTime = System.nanoTime() - startTime;
        return estimatedTime;
    }

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD    a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName  Halyard repository/HBase table
     * @param configPath HBase config path
     * @return how long the processing took in nano-seconds
     */
    public static long saveToHalyard(JavaRDD<Object> javaRDD, String tableName, String configPath) throws IOException {
        return HalyardRDD.saveToHalyard(javaRDD, tableName, configPath, null);
    }

    /**
     * Collects and save Plaso (Event, EventData) pairs into Halyard as RDF triplets.
     *
     * @param javaRDD   a JavaRDD object of Plaso (Event, EventData) pairs
     * @param tableName Halyard repository/HBase table
     * @return how long the processing took in nano-seconds
     */
    public static long saveToHalyard(JavaRDD<Object> javaRDD, String tableName) throws IOException {
        return HalyardRDD.saveToHalyard(javaRDD, tableName, null, null);
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
