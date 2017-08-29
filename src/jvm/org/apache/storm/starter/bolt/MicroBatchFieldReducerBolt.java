package org.apache.storm.starter.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class MicroBatchFieldReducerBolt implements IRichBolt {
    private OutputCollector collector;
    private static final Logger LOG = Logger.getLogger(MicroBatchFieldReducerBolt.class);

    /**
     * The queue holding tuples in a batch.
     */
    protected LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<>();

    /**
     * The threshold after which the batch should be flushed out.
     */
    int batchSize = 100;

    /**
     * The batch interval in sec. Minimum time between flushes if the batch sizes
     * are not met. This should typically be equal to
     * topology.tick.tuple.freq.secs and half of topology.message.timeout.secs
     */
    //int batchIntervalInSec = 45;
    int batchIntervalInSec = 15;

    /**
     * The last batch process time seconds. Used for tracking purpose
     */
    long lastBatchProcessTimeSeconds = 0;


    @Override
    public void execute(Tuple tuple) {
        // Check if the tuple is of type Tick Tuple

        if (isTickTuple(tuple)) {
            // If so, it is indication for batch flush. But don't flush if previous
            // flush was done very recently (either due to batch size threshold was
            // crossed or because of another tick tuple
            //

            if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {
                LOG.debug("Current queue size is " + this.queue.size()
                        + ". But received tick tuple so executing the batch");
                finishBatch();

            } else {
                LOG.debug("Current queue size is " + this.queue.size()
                        + ". Received tick tuple but last batch was executed "
                        + (System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds)
                        + " seconds back that is less than " + batchIntervalInSec
                        + " so ignoring the tick tuple");
            }

        } else {
            // Add the tuple to queue. But don't ack it yet.
            this.queue.add(tuple);

            int queueSize = this.queue.size();
            LOG.debug("current queue size is " + queueSize);

            if (queueSize >= batchSize) {
                LOG.debug("Current queue size is >= " + batchSize
                        + " executing the batch");

                finishBatch();
            }
        }
    }


    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(
                Constants.SYSTEM_TICK_STREAM_ID);
    }

    private String processTuple(String[] inpArr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("#### %s|%s|%s|%s|%s", inpArr[0], inpArr[4], inpArr[5], inpArr[11], inpArr[17]));
        }

        // Get the relevant fields, as a single field (KafkaBolt needs single field)
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", inpArr[0], inpArr[4], inpArr[5], inpArr[6], inpArr[10],
                inpArr[11], inpArr[17], inpArr[23], inpArr[25], inpArr[34], inpArr[36], inpArr[41]);
    }

    /**
     * Finish batch.
     */
    public void finishBatch() {

        LOG.debug("Finishing batch of size " + queue.size());
        lastBatchProcessTimeSeconds = System.currentTimeMillis() / 1000;

        List<Tuple> tuples = new ArrayList<>();
        queue.drainTo(tuples);

        //BulkRequestBuilder bulkRequest = client.prepareBulk();
        //BulkResponse bulkResponse = null;

        for (Tuple tuple : tuples) {

            String inputRecord = tuple.getString(0);
            String[] inpArr = inputRecord.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            inpArr = StringUtils.stripAll(inpArr, "\"");

            // ignore headers
            if (inpArr[0].equalsIgnoreCase("Year")) {
                collector.ack(tuple);
                continue;
            }
            collector.emit(new Values(new Object[]{processTuple(inpArr)}));
            collector.ack(tuple);

        }

//        try {
//            // Execute bulk request and get individual tuple responses back.
//            bulkResponse = bulkRequest.execute().actionGet();
//            BulkItemResponse[] responses = bulkResponse.getItems();
//            BulkItemResponse response = null;
//
//            LOG.debug("Executed the batch. Processing responses.");
//            for (int counter = 0; counter < responses.length; counter++) {
//                response = responses[counter];
//
//                if (response.isFailed()) {
//                    ElasticSearchDocument failedEsDocument = this.tupleMapper
//                            .mapToDocument(tuples.get(counter));
//                    LOG.error("Failed to process tuple # " + counter);
//                    this.collector.fail(tuples.get(counter));
//
//                } else {
//                    LOG.debug("Successfully processed tuple # " + counter);
//                    this.collector.ack(tuples.get(counter));
//                }
//            }
//
//        } catch (Exception e) {
//            LOG.error("Unable to process " + tuples.size() + " tuples", e);
//
//            // Fail entire batch
//            for (Tuple tuple : tuples) {
//                this.collector.fail(tuple);
//            }
//
//        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(new String[]{"output"}));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void cleanup() {
        queue.clear();
    }
}