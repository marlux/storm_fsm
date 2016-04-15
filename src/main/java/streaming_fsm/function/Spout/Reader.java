package streaming_fsm.function.Spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import streaming_fsm.api.SearchSpaceItem;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

public class Reader extends BaseRichSpout {

    public static final String ITEM_STREAM = "reportStream";
    public static final String ITEM_DISTRIBUTION_FINISHED_STREAM = "phase";
    public static final String ITEM_DATA_FIELD = "data";
    public static final String ITEM_ID_FIELD = "seqId";
    public static final String ITEM_DISTRIBUTION_FINISHED_FIELD = "done";

    List<SearchSpaceItem> data = new ArrayList<>();

    boolean finished = false;

    /**
   * sends tuples
   */
  SpoutOutputCollector spoutOutputCollector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Sendet die Daten
        outputFieldsDeclarer.declareStream(
          ITEM_STREAM, new Fields(ITEM_DATA_FIELD, ITEM_ID_FIELD));

        // Sagt das alle Graphen gesendet wurden
        outputFieldsDeclarer.declareStream(ITEM_DISTRIBUTION_FINISHED_STREAM,
          new Fields(ITEM_DISTRIBUTION_FINISHED_FIELD));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext,
      SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        // prevents tuples from getting lost
        Utils.sleep(10);

        if (! finished) {

            // for each search space item
            for (int i = 0; i < data.size(); i++) {
                // send tuple
                spoutOutputCollector.emit(
                  ITEM_STREAM, new Values(data.get(i), i));
            }
            // send distribution finished
            spoutOutputCollector.emit(
              ITEM_DISTRIBUTION_FINISHED_STREAM, new Values(true));

            finished = true;
        }

        Utils.sleep(100);
    }

    public void setData(List<SearchSpaceItem> data) {
        this.data = data;
    }
}
