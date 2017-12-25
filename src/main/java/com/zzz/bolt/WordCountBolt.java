package com.zzz.bolt;

import com.google.common.collect.Maps;
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
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private Map<String, Long> wordCountMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        wordCountMap = Maps.newHashMap();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = wordCountMap.get(word);

        count = count == null ? 0L : ++count;

        wordCountMap.put(word, count);

        outputCollector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
