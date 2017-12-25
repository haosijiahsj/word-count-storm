package com.zzz.bolt;

import com.google.common.base.Splitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hushengjun on 2017/12/25.
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        Splitter.on(" ").splitToList(sentence).forEach(word -> outputCollector.emit(new Values(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

}
