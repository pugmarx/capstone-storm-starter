package org.apache.storm.starter;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.bolt.DupRemoverBolt;
import org.apache.storm.starter.bolt.HashAdderBolt;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;


public class KafkaHashBasedTopology {
    private static final String ZK_LOCAL_HOST = "localhost:2181";

    //private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:2181";
    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "testclean";
    private static final String BROKER_URL = "localhost:9092";
    //private static final String ZK_ROOT = "/brokers";
    private static final String ZK_ROOT = "/consumers";
    //private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final String CLIENT_ID = "storm-local-hash";
    private static final int PARALLELISM_HINT = 1;
    private static final int NUM_TASK_PER_ENTITY = 1;
    private static final int NUM_WORKERS = 1;

    private static final Logger LOG = Logger.getLogger(KafkaHashBasedTopology.class);

    private static void init(Config config) {
        //Config config = new Config();
        String configKey = "cassandra-config";
        HashMap<String, Object> clientConfig = new HashMap<>();
        clientConfig.put("cassandra.nodes", Arrays.asList(new String[]{"localhost"}));
        clientConfig.put("cassandra.port", "9160");
        clientConfig.put("cassandra.keyspace", Arrays.asList(new String[]{"stormks"}));
        config.put(configKey, clientConfig);
    }

    public static void main(String[] args) throws Exception {

        String zkHost = (args.length > 0 && StringUtils.isNotEmpty(args[0])) ? args[0] : ZK_LOCAL_HOST;
        final BrokerHosts ZK_HOSTS = new ZkHosts(zkHost);

        String brokerURL = (args.length > 1 && StringUtils.isNotEmpty(args[1])) ? args[1] : BROKER_URL;
        String inTopic = (args.length > 2 && StringUtils.isNotEmpty(args[2])) ? args[2] : INPUT_TOPIC;
        String outTopic = (args.length > 3 && StringUtils.isNotEmpty(args[3])) ? args[3] : OUTPUT_TOPIC;
        String clientId = (args.length > 4 && StringUtils.isNotEmpty(args[4])) ? args[4] : CLIENT_ID;

        final SpoutConfig kafkaConf = new SpoutConfig(ZK_HOSTS, inTopic, ZK_ROOT, clientId);
        kafkaConf.retryLimit = 0;
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();

        // FIXME - disable debug
        Config config = new Config();
        config.setDebug(false);

        // FIXME experimental
        config.setNumWorkers(NUM_WORKERS);

        // Initialize persistence
        init(config);


        // ********************************************************************************
        // ********************* 1. Spout that reads from Kafka ***************************
        // ********************************************************************************
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        topologyBuilder.setSpout("kafka-spout", kafkaSpout, PARALLELISM_HINT).setNumTasks(NUM_TASK_PER_ENTITY);


        // ********************************************************************************
        // ********************** 2. Bolt that reads from Spout ***************************
        // ********************************************************************************
        topologyBuilder.setBolt("add-hash", new HashAdderBolt(), PARALLELISM_HINT)
                .setNumTasks(NUM_TASK_PER_ENTITY)
                .globalGrouping("kafka-spout");


        // ********************************************************************************
        // ********************** 2. Bolt that checks for duplicates **********************
        // ********************************************************************************
        topologyBuilder.setBolt("remove-dups", new DupRemoverBolt(), PARALLELISM_HINT)
                .setNumTasks(NUM_TASK_PER_ENTITY)
                .shuffleGrouping("add-hash");
                //.globalGrouping("add-hash");


        // ********************************************************************************
        // ********************* 4. Bolt that writes to another Kafka topic ***************
        // ********************************************************************************
        KafkaBolt<String, String> bolt = (new KafkaBolt()).withProducerProperties(newProps(brokerURL,
                outTopic))
                .withTopicSelector(new DefaultTopicSelector(outTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key",
                        "output"));

        // FIXME don't want ACKs
        bolt.setFireAndForget(true);

        // Tie the kafkabolt to reduce-field bolt
        topologyBuilder.setBolt("kafka-producer-bolt", bolt, PARALLELISM_HINT).setNumTasks(NUM_TASK_PER_ENTITY)
                .shuffleGrouping("remove-dups");

        // Submit topology to local cluster // FIXME cluster?
        final LocalCluster localCluster = new LocalCluster();
        String topologyName = "kafka-hash-local-topology";
        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
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