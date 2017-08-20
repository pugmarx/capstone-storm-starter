package org.apache.storm.starter.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class GreetSpitterBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] langGreeting = sentence.split("\\s");
        collector.emit(new Values(langGreeting[1], langGreeting[0]));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word"));
        declarer.declare(new Fields("greetingField", "langField"));
    }

    @Override
    public void cleanup() {
        System.out.println("-------------- splitter exit ---------------");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}