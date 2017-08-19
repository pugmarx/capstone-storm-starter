package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.GreetCombinerBolt;
import org.apache.storm.starter.bolt.GreetSpitterBolt;
import org.apache.storm.starter.spout.LineReaderSpout;
import org.apache.storm.topology.TopologyBuilder;

public class GreeterTopology {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        //config.put("inputFile", args[0]);
        config.put("inputFile", "multilang/resources/hello.txt");
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("greet-reader-spout", new LineReaderSpout());
        builder.setBolt("greet-spitter", new GreetSpitterBolt()).shuffleGrouping("greet-reader-spout");
        builder.setBolt("greet-combiner", new GreetCombinerBolt()).shuffleGrouping("greet-spitter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("GreeterTopology", config, builder.createTopology());
        Thread.sleep(10000);

        cluster.shutdown();
    }

}