package org.apache.storm.starter;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.starter.bolt.GreetCombinerBolt;
import org.apache.storm.starter.bolt.GreetSpitterBolt;
import org.apache.storm.starter.spout.LineReaderSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class GreeterTopology {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        //config.put("inputFile", args[0]);
        config.put("inputFile", "multilang/resources/hello.txt");
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("greet-reader-spout", new LineReaderSpout());

        builder.setBolt("greet-spitter", new GreetSpitterBolt(), 3).shuffleGrouping("greet-reader-spout");

        // Send different greetings to separate bolt tasks
        builder.setBolt("greet-combiner", new GreetCombinerBolt(), 3).fieldsGrouping("greet-spitter",
                new Fields("greetingField"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("GreeterTopology", config, builder.createTopology());
        Thread.sleep(10000);

        cluster.shutdown();
    }

}