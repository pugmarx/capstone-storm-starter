package org.apache.storm.starter;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.bolt.MicroBatchFieldReducerBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class KafkaClusterReaderTopology {

    private static final String ZK_LOCAL_HOST = "localhost:2181";
    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "testclean";
    private static final String BROKER_URL = "localhost:9092";
    private static final String ZK_ROOT = "/brokers";
    private static final String CLIENT_ID = UUID.randomUUID().toString();


    private static final Logger LOG = Logger.getLogger(KafkaClusterReaderTopology.class);

    public static void main(String[] args) throws Exception {

        String zkHost = (args.length > 0 && StringUtils.isNotEmpty(args[0])) ? args[0] : ZK_LOCAL_HOST;
        final BrokerHosts ZK_HOSTS = new ZkHosts(zkHost);

        String brokerURL = (args.length > 1 && StringUtils.isNotEmpty(args[1])) ? args[1] : BROKER_URL;
        String inTopic = (args.length > 2 && StringUtils.isNotEmpty(args[2])) ? args[2] : INPUT_TOPIC;
        String outTopic = (args.length > 3 && StringUtils.isNotEmpty(args[3])) ? args[3] : OUTPUT_TOPIC;
        String clientId = (args.length > 4 && StringUtils.isNotEmpty(args[4])) ? args[4] : CLIENT_ID;

        //final SpoutConfig kafkaConf = new SpoutConfig(ZK_HOSTS, INPUT_TOPIC, ZK_ROOT, CLIENT_ID);
        final SpoutConfig kafkaConf = new SpoutConfig(ZK_HOSTS, inTopic, ZK_ROOT, clientId);

        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        // Build topology to consume message from kafka and print them on console
        final TopologyBuilder topologyBuilder = new TopologyBuilder();

        // FIXME - disable debug
        Config config = new Config();
        config.setDebug(false);

        // Create KafkaSpout instance using Kafka configuration and add it to topology
        //topologyBuilder.setSpout("kafka-spout", new KafkaSpout<>(KafkaSpoutConfig.builder(KAFKA_BOOTSTRAP_SERVER,
        //        IN_TOPIC).build()), 1);

        // ********************************************************************************
        // ********************* 1. Spout that reads from Kafka ***************************
        // ********************************************************************************
        // FIXME improve parallelism??
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
        topologyBuilder.setSpout("kafka-spout", kafkaSpout, 1);


        // ********************************************************************************
        // ********************** 2. Bolt that reads from Spout ***************************
        // ********************************************************************************
        //Route the output of Kafka Spout to Logger bolt to log messages consumed from Kafka
        // FIXME improve parallelism??
        //topologyBuilder.setBolt("reduce-fields", new FieldReducerBolt())
        //       .globalGrouping("kafka-spout");
        topologyBuilder.setBolt("reduce-fields", new MicroBatchFieldReducerBolt())
                .globalGrouping("kafka-spout");

        // ********************************************************************************
        // ********************* 3. Bolt that writes to another Kafka topic ***************
        // ********************************************************************************
        // FIXME improve parallelism??
        KafkaBolt<String, String> bolt = (new KafkaBolt()).withProducerProperties(newProps(brokerURL,
                outTopic))
                .withTopicSelector(new DefaultTopicSelector(outTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key",
                        "output"));

        //.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "str")); <-- this works with KafkaSpout!!

        // Tie the kafkabolt to reduce-field bolt
        topologyBuilder.setBolt("kafka-producer-bolt", bolt).shuffleGrouping("reduce-fields");

        //** experimental ** topologyBuilder.setBolt("kafka-producer-bolt", bolt).shuffleGrouping("kafka-spout");

        // Submit topology to local cluster
        // FIXME - enable cluster deployment
        //final LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("kafka-topology", config, topologyBuilder.createTopology());

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);

        String name = "kafka-cluster-topology";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(name, conf, topologyBuilder.createTopology());

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        // FIXME Sleep for 5 mins
        for (int i = 0; i < 10; i++) {
            LOG.info("---------- Sleeping for 5 mins -----------------");
            Thread.sleep(30 * 1000);
            printMetrics(client, name);
        }
        kill(client, name);
    }


    private static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts : summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named " + name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec : info.get_executors()) {
            if ("spout".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key : ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal / acked;
        System.out.println("uptime: " + uptime + " acked: " + acked + " avgLatency: " + avgLatency + " acked/sec: "
                + (((double) acked) / uptime + " failed: " + failed));
    }

    private static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
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