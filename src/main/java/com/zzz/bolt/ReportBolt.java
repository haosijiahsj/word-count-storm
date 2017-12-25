package com.zzz.bolt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by hushengjun on 2017/12/25.
 */
@Slf4j
public class ReportBolt extends BaseRichBolt {

    private Map<String, Long> wordCountMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.wordCountMap = Maps.newHashMap();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");

        wordCountMap.put(word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        log.info("final result...");

        List<String> keys = Lists.newArrayList(this.wordCountMap.keySet());
        Collections.sort(keys);

        keys.forEach(key -> {
            Long count = wordCountMap.get(key);
            log.info("word: {}, count: {}", key, count);
        });
    }
}
