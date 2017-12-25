package com.zzz.topology;

import com.zzz.bolt.ReportBolt;
import com.zzz.bolt.SplitSentenceBolt;
import com.zzz.bolt.WordCountBolt;
import com.zzz.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by hushengjun on 2017/12/25.
 */
public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPILL_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout());
        topologyBuilder.setBolt(SPILL_BOLT_ID, new SplitSentenceBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLT_ID, new WordCountBolt()).fieldsGrouping(SPILL_BOLT_ID, new Fields("word"));
        topologyBuilder.setBolt(REPORT_BOLT_ID, new ReportBolt()).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
        localCluster.killTopology(TOPOLOGY_NAME);
        localCluster.shutdown();
    }

}
