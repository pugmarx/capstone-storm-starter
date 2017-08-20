package org.apache.storm.starter;


import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.bolt.FieldReducerBolt;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;


public class KafkaSampleTopology {

    private static final BrokerHosts ZK_HOSTS = new ZkHosts("localhost:2181");
    private static final String KAFKA_TOPIC = "test";
    private static final String ZK_ROOT = "/brokers";
    private static final String CLIENT_ID = UUID.randomUUID().toString();

    private static final Logger LOG = Logger.getLogger(KafkaSampleTopology.class);

    public static void main(String[] args) {

        final SpoutConfig kafkaConf = new SpoutConfig(ZK_HOSTS, KAFKA_TOPIC, ZK_ROOT, CLIENT_ID);

        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();

        // FIXME - disable debug
        Config config = new Config();
        config.setDebug(true);

        // Create KafkaSpout instance using Kafka configuration and add it to topology
        topologyBuilder.setSpout("kafka-spout", new KafkaSpout(kafkaConf), 1);


        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        topologyBuilder.setBolt("print-messages", new FieldReducerBolt(), 5).globalGrouping("kafka-spout");

        // Submit topology to local cluster i.e. embedded storm instance in eclipse
        // FIXME - enable cluster deployment
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-topology", config, topologyBuilder.createTopology());
    }

}