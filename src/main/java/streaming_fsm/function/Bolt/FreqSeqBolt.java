package streaming_fsm.function.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import streaming_fsm.function.Helper.ResultHolder;
import streaming_fsm.interfaces.Pattern;

import java.util.Map;

/**
 * Created by marlux on 06.01.16.
 */
public class FreqSeqBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    static ResultHolder result;
    private Integer counter = 0;
    private Integer numberOfSplittersInstances = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        switch (tuple.getSourceStreamId()) {
            case "done":
                counter++;
                if (counter >= this.numberOfSplittersInstances) ;
                result.done = true;
                break;
            case "frequent":
                Pattern freqSeq = (Pattern) tuple.getValueByField("seq");
                result.add(freqSeq);
                outputCollector.ack(tuple);
                break;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void setResult(ResultHolder result) {
        this.result=result;
    }

    public void setNumberOfSplitterInstances(Integer number) {
        this.numberOfSplittersInstances = number;
    }

}
