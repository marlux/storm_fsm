package streaming_fsm.function.Spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import streaming_fsm.interfaces.SearchSpaceItem;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

/**
 * Created by marlux on 06.01.16.
 */
public class FreqListDataSpout extends BaseRichSpout {

    List<SearchSpaceItem> data = new ArrayList<>();
    SpoutOutputCollector spoutOutputCollector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        // Sendet die Daten
        outputFieldsDeclarer.declareStream("splittedData", new Fields("data", "seqId"));

        // Sagt das alle Graphen gesendet wurden
        outputFieldsDeclarer.declareStream("phase", new Fields("done"));
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
                        .emit("splittedData", new Values(data.get(i), i));
            }
            spoutOutputCollector.emit("phase", new Values("1"));
            test = false;
        }

        Utils.sleep(100);


    }

    public void setData(List<SearchSpaceItem> data) {
        this.data = data;
    }
}
