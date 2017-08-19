package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.LineSplitterBolt;
import org.apache.storm.starter.bolt.WordCounterBolt;
import org.apache.storm.starter.spout.LineReaderSpout;
import org.apache.storm.topology.TopologyBuilder;

import java.io.File;

public class LineProcessorTopology {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        //config.put("inputFile", args[0]);
        config.put("inputFile", "multilang/resources/countryhello.txt");
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("line-reader-spout", new LineReaderSpout());
        builder.setBolt("word-spitter", new LineSplitterBolt()).shuffleGrouping("line-reader-spout");
        builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spitter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LineProcessorTopology", config, builder.createTopology());
        Thread.sleep(10000);

        cluster.shutdown();
    }

}