package streaming_fsm.function.Bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import streaming_fsm.interfaces.Pattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by marlux on 06.01.16.
 */
public class FreqAggSeqBolt extends BaseRichBolt {

    float min_support;
    Integer number_of_elements;
    Map<Pattern, ArrayList<Integer>> seqCounter = new HashMap<>();
    Map<Integer, Integer> phasesDone = new HashMap<>();
    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        switch (tuple.getSourceStreamId()) {
            case "phase":
                Integer size = tuple.getIntegerByField("size");
                Integer phase = tuple.getIntegerByField("phase");

                if (phasesDone.containsKey(phase)) {
                    size = phasesDone.get(phase) + size;
                }

                phasesDone.put(phase, size);

                break;
            case "element":

                Integer Seq = tuple.getIntegerByField("seqId");
                Pattern Subseq = (Pattern) tuple.getValueByField("element");

                // Subseq wurde schonmal gefunden
                if (seqCounter.containsKey(Subseq)) {
                    ArrayList<Integer> current = new ArrayList<>(seqCounter.get(Subseq));
                    if (!current.contains(Seq)) {
                        current.add(Seq);
                    }
                    seqCounter.put(Subseq, current);
                } else {
                    ArrayList<Integer> init = new ArrayList<>();
                    init.add(Seq);
                    seqCounter.put(Subseq, init);
                }

                ArrayList<Integer> current = new ArrayList<>(seqCounter.get(Subseq));

                if (((float) current.size()) / number_of_elements >= min_support) {
                    outputCollector.emit("frequent", new Values(Subseq));
                    outputCollector.emit("frequent2", new Values(Subseq));
                } else {
                    if (phasesDone.containsKey(Subseq.size())) {

                        float freqpos = ((float) current.size() + (number_of_elements - phasesDone.get(Subseq.size())));
                        // Ist der branch infrequent?
                        if ((freqpos / number_of_elements) < min_support) {
                            outputCollector.emit("infrequent", new Values(Subseq));
                        }
                    }
                }
                break;
        }

        outputCollector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("frequent", new Fields("seq"));
        outputFieldsDeclarer.declareStream("frequent2", new Fields("seq"));
        outputFieldsDeclarer.declareStream("infrequent", new Fields("seq"));
    }

    public void set_min_support(float support) {
        this.min_support = support;
    }

    public void setNumberOfElements(Integer size) {
        this.number_of_elements = size;
    }
}
