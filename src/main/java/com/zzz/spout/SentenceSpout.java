package com.zzz.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hushengjun on 2017/12/25.
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't hava a cow man ",
            "i don't think i like fleas"
    };

    private int index = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (this.index < sentences.length) {
            spoutOutputCollector.emit(new Values(sentences[index]));
            index++;
        } else {
            index = 0;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

}
