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


public class FieldReducerBolt implements IRichBolt {
    private OutputCollector collector;
    Fields outFields = null;
    private static final Logger LOG = Logger.getLogger(FieldReducerBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String inputRecord = input.getString(0);
            String[] inpArr = inputRecord.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            inpArr = StringUtils.stripAll(inpArr, "\"");
            //LOG.debug(String.format("#### %s|%s|%s|%s|%s", inpArr[0], inpArr[4], inpArr[5], inpArr[11], inpArr[17]));
            collector.emit(new Values(inpArr[0], inpArr[4], inpArr[5], inpArr[11], inpArr[17]));
            collector.ack(input);

        } catch (Exception aex) {
            System.out.println("--- ignore bad msg ---");
            collector.ack(input);
            return;
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // declarer.declare(outFields);
    }

    @Override
    public void cleanup() {
        System.out.println("-------------- FieldReducerBolt exit ---------------");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    public static void main(String s[]) throws Exception {

        String csString = "a,b,c,\"d\",\"e,f\"";
        String[] splitted = csString.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
        splitted = StringUtils.stripAll(splitted, "\"");
        for (String str : splitted) {
            System.out.println(str);
        }


    }
}