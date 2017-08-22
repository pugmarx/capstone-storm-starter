package org.apache.storm.starter;


import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
//import org.apache.storm.kafka.bolt.KafkaBolt;
//import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
//import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
//import org.apache.storm.kafka.spout.KafkaSpout;
//import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
//import org.apache.storm.starter.bolt.FieldReducerBolt;
import org.apache.storm.starter.bolt.FieldReducerBolt;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;
import java.util.UUID;


public class KafkaSampleTopology {

    private static final BrokerHosts ZK_HOSTS = new ZkHosts("localhost:2181");
    //private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:2181";
    private static final String KAFKA_TOPIC = "test";
    private static final String ZK_ROOT = "/brokers";
    private static final String CLIENT_ID = UUID.randomUUID().toString();

    private static final Logger LOG = Logger.getLogger(KafkaSampleTopology.class);

    public static void main(String[] args) {

        final SpoutConfig kafkaConf = new SpoutConfig(ZK_HOSTS, KAFKA_TOPIC, ZK_ROOT, CLIENT_ID);
        //kafkaConf.
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();

        // FIXME - disable debug
        Config config = new Config();
        config.setDebug(true);

        // Create KafkaSpout instance using Kafka configuration and add it to topology
        //topologyBuilder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfig.builder(KAFKA_BOOTSTRAP_SERVER,
        //        KAFKA_TOPIC).build()), 1);

        // ********************************************************************************
        // ********************* 1. Spout that reads from Kafka ***************************
        // ********************************************************************************
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        topologyBuilder.setSpout("kafka-spout", kafkaSpout, 1);


        // ********************************************************************************
        // ********************** 2. Bolt that reads from Spout ***************************
        // ********************************************************************************
        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        topologyBuilder.setBolt("reduce-fields", new FieldReducerBolt())
                .globalGrouping("kafka-spout");

        // ********************************************************************************
        // ********************* 3. Bolt that writes to another Kafka topic ***************
        // ********************************************************************************
        KafkaBolt<String, String> bolt = (new KafkaBolt()).withProducerProperties(newProps("localhost:9092",
                "testclean"))
                .withTopicSelector(new DefaultTopicSelector("testclean"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "output"));

        //.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "str")); <-- this works with KafkaSpout!!

        // Tie the kafkabolt to reduce-field bolt
        topologyBuilder.setBolt("kafka-producer-bolt", bolt).shuffleGrouping("reduce-fields");


        //** experimental ** topologyBuilder.setBolt("kafka-producer-bolt", bolt).shuffleGrouping("kafka-spout");
        /***************************/

        // Submit topology to local cluster
        // FIXME - enable cluster deployment
        final LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafka-topology", config, topologyBuilder.createTopology());
    }

    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {
            {
                this.put("bootstrap.servers", brokerUrl);
                this.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                this.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                this.put("client.id", topicName);
            }
        };
    }

}