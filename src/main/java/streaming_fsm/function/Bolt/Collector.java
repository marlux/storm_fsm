package streaming_fsm.function.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import streaming_fsm.function.Helper.ResultHolder;
import streaming_fsm.api.Pattern;

import java.util.Map;

public class Collector extends BaseRichBolt {
    OutputCollector outputCollector;

  /**
   * TODO: Why static?
   */
  static ResultHolder result;
    private Integer numberOfFinishedGrowers = 0;
    private Integer numberOfGrowers = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
      OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        boolean processedTuple = false;

        switch (tuple.getSourceStreamId()) {
            case Grower.THREAD_DONE_STREAM:
                numberOfFinishedGrowers++;
                if (numberOfFinishedGrowers >= this.numberOfGrowers) ;
                result.done = true;
                processedTuple = true;
                break;
            case Aggregator.COLLECT_STREAM:
                Pattern freqSeq = (Pattern) tuple.getValueByField(Aggregator
                  .PATTERN_FIELD);
                result.add(freqSeq);
                processedTuple = true;
                break;
        }

        if(processedTuple) {
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void setResult(ResultHolder result) {
        this.result = result;
    }

    public void setNumberOfSplitterInstances(Integer number) {
        this.numberOfGrowers = number;
    }

}
