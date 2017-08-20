package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.FieldReducerBolt;
import org.apache.storm.starter.bolt.GreetCombinerBolt;
import org.apache.storm.starter.bolt.GreetSpitterBolt;
import org.apache.storm.starter.spout.CSVDataSpout;
import org.apache.storm.starter.spout.LineReaderSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class CSVSampleTopology {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();

        String fpath = "multilang/resources/sample_1998.csv";
        builder.setSpout("csv-reader-spout", new CSVDataSpout(fpath, ',', true));

        builder.setBolt("field-reducer", new FieldReducerBolt(), 3).shuffleGrouping("csv-reader-spout");

        // Send different greetings to separate bolt tasks
        //builder.setBolt("greet-combiner", new GreetCombinerBolt(), 3).fieldsGrouping("greet-spitter",
        //        new Fields("greetingField"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CSVSampleTopology", config, builder.createTopology());
        Thread.sleep(10000);

        cluster.shutdown();
    }

}