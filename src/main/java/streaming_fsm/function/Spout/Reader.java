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

/**
 * Created by marlux on 06.01.16.
 */
public class Reader extends BaseRichSpout {

    public static final String ITEM_STREAM = "reportStream";
    public static final String PHASE_STREAM = "phase";

    List<SearchSpaceItem> data = new ArrayList<>();
    SpoutOutputCollector spoutOutputCollector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        // Sendet die Daten
        outputFieldsDeclarer.declareStream(
          ITEM_STREAM, new Fields("data", "seqId"));

        // Sagt das alle Graphen gesendet wurden
        outputFieldsDeclarer.declareStream(PHASE_STREAM, new Fields("done"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;
    }

    boolean test = true;

    @Override
    public void nextTuple() {
        Utils.sleep(10);

        if (test) {
            for (int i = 0; i < data.size(); i++) {
                spoutOutputCollector
                        .emit(ITEM_STREAM, new Values(data.get(i), i));
            }
            spoutOutputCollector.emit(PHASE_STREAM, new Values("1"));
            test = false;
        }

        Utils.sleep(100);


    }

    public void setData(List<SearchSpaceItem> data) {
        this.data = data;
    }
}
