package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


public class GreetCombinerBolt implements IRichBolt {
    Map<String, String> greetMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.greetMap = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String greet = input.getStringByField("greetingField");
        String lang = input.getStringByField("langField");

        if (!greetMap.containsKey(greet)) {
            greetMap.put(greet, lang);
            //greetMap.put(str, 1);
        } else {
            String g = greetMap.get(greet) + "," + lang;
            greetMap.put(greet, g);
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
        for (Map.Entry<String, String> entry : greetMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}