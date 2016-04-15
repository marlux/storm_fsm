package streaming_fsm;

import java.util.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


import streaming_fsm.function.Bolt.FreqSeqBolt;
import streaming_fsm.function.Bolt.FreqAggSeqBolt;
import streaming_fsm.function.Bolt.FreqSplitDataBolt;
import streaming_fsm.function.Helper.ResultHolder;
import streaming_fsm.function.Spout.FreqListDataSpout;
import streaming_fsm.interfaces.Pattern;
import streaming_fsm.interfaces.SearchSpaceItem;


/**
 * Created by marlux on 06.01.16.
 */
public class SeqStreamer {

    ArrayList<Pattern> result = new ArrayList<>();
    ArrayList<SearchSpaceItem> input = new ArrayList<>();

    Integer maxExecutionTime = 10000;
    Integer loopExecutionTime = 1000;
    Integer currentExecutionTime = 0;

    Integer numberOfSplitterInstances = 3;
    Integer numberOfAggregatorInstances = 3;

    float min_support = 0;

    public void setNumberOfAggregatorInstances(Integer numberOfAggregatorInstances) {
        this.numberOfAggregatorInstances = numberOfAggregatorInstances;
    }

    public void setMaxExecutionTime(Integer maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    public void setNumberOfSplitterInstances(Integer numberOfSplitterInstances) {
        this.numberOfSplitterInstances = numberOfSplitterInstances;
    }

    /**
     * Fügt der Klasse den Input hinzu
     *
     * @param input Liste von Sequenzen
     */
    public void setInput(ArrayList<SearchSpaceItem> input) {
        this.input = input;
    }

    /**
     * Gibt das Ergebnis der Berechnung zurück
     *
     * @return Ergebnis der Sequenzbestimmung
     */
    public List<Pattern> getResult() {
        return result;
    }

    /**
     * Setzt den benötigten min_support
     *
     * @param min_support
     */
    public void setMin_support(float min_support) {
        this.min_support = min_support;
    }


    /**
     * Berechnung
     */
    public void compute() {
        TopologyBuilder builder = new TopologyBuilder();

        // 1
        FreqListDataSpout freqListDataSpout = new FreqListDataSpout();
        freqListDataSpout.setData(input);

        builder.setSpout("1", freqListDataSpout);

        // 2
        builder.setBolt("2", new FreqSplitDataBolt(), this.numberOfSplitterInstances)
                .shuffleGrouping("1", "splittedData")
                .allGrouping("1", "phase")
                .allGrouping("3", "infrequent")
                .allGrouping("3", "frequent2");

        // 3
        FreqAggSeqBolt freqAggSeqBolt = new FreqAggSeqBolt();
        freqAggSeqBolt.setNumberOfElements(input.size());
        freqAggSeqBolt.set_min_support(min_support);

        System.out.println("Support:" + min_support);
        builder.setBolt("3", freqAggSeqBolt, this.numberOfAggregatorInstances)
                .fieldsGrouping("2", "element", new Fields("element"))
                .allGrouping("2", "phase");

        // 4
        FreqSeqBolt freqSeqBolt = new FreqSeqBolt();
        ResultHolder er = new ResultHolder();
        freqSeqBolt.setResult(er);
        freqSeqBolt.setNumberOfSplitterInstances(numberOfSplitterInstances);

        builder.setBolt("4", freqSeqBolt).shuffleGrouping("3", "frequent")
                .shuffleGrouping("2", "done");

        // https://issues.apache.org/jira/browse/FLINK-2836
        // zyklische Graphen sind in der Kompabilität nicht möglich
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);

        try {
            cluster.submitTopology("Async Sequence Computioner", conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

        while(!er.done && maxExecutionTime > currentExecutionTime) {
            Utils.sleep(loopExecutionTime);
            currentExecutionTime += loopExecutionTime;
        }

        if(currentExecutionTime >= maxExecutionTime && !er.done) {
            System.out.println("MAX EXECUTION TIME REACHED BUT NOT DONE");
            cluster.shutdown();
            System.exit(0);
        }

        this.result = er.getResult();

        System.out.println("Size: " + this.result.size());

        cluster.shutdown();
    }
}
