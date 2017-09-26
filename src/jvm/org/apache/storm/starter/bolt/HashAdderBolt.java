package org.apache.storm.starter.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class HashAdderBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(HashAdderBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String inputRecord = input.getString(0);
            String hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(inputRecord);
            String output = String.format("%s|%s", hash, inputRecord);
            collector.emit(new Values(new Object[]{output}));
            collector.ack(input);
        } catch (Exception aex) {
            LOG.error("Add-hash:: ignoring tuple ::" + input);
            collector.ack(input);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"output"}));
    }

    @Override
    public void cleanup() {
        LOG.info("-------------- FieldReducerBolt exit ---------------");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}