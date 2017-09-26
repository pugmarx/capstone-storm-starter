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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DupRemoverBolt implements IRichBolt {
    private OutputCollector collector;
    //Fields outFields = null;
    private static final Logger LOG = Logger.getLogger(DupRemoverBolt.class);
    private static Set<String> keySet;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.keySet = new HashSet();
    }

    private boolean shouldIgnoreKey(String hash) {
        return keySet.contains(hash);
    }

    @Override
    public void execute(Tuple input) {
        try {

            String inputRecord = input.getStringByField("output");
            String[] hashWithRec = inputRecord.split("\\|");

            if (shouldIgnoreKey(hashWithRec[0])) {
                LOG.error("----- Ignoring previously received tuple ------" + hashWithRec[0]);
                collector.ack(input);
                return;
            }

            addKey(hashWithRec[0]);

            String[] inpArr = hashWithRec[1].split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            inpArr = StringUtils.stripAll(inpArr, "\"");

            // Strip header
            if (inpArr[0].equals("Year")) {
                collector.ack(input);
                return;
            }

            // Emit the relevant fields, as a single field (KafkaBolt needs single field)
            // 0,5,23,4,6,10,11,17,25,34,36,41
            String output = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", inpArr[0], inpArr[5], inpArr[23],
                    inpArr[4], inpArr[6], inpArr[10], inpArr[11], inpArr[17], inpArr[25], inpArr[34], inpArr[36],
                    inpArr[41]);
            collector.emit(new Values(new Object[]{output}));
            collector.ack(input);

        } catch (Exception aex) {
            LOG.error("DupRemover :: Ignoring tuple ::" + input, aex);
            collector.ack(input);
        }
    }

    private boolean addKey(String key){
        keySet.add(key);
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"output"}));
    }

    @Override
    public void cleanup() {
        //if (LOG.isInfoEnabled()) {
        LOG.info("-------------- FieldReducerBolt exit ---------------");
        //}
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