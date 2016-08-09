package streaming_fsm;

import java.util.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


import streaming_fsm.function.Bolt.Collector;
import streaming_fsm.function.Bolt.Aggregator;
import streaming_fsm.function.Bolt.Grower;
import streaming_fsm.function.Helper.ResultHolder;
import streaming_fsm.function.Spout.Reader;
import streaming_fsm.api.Pattern;
import streaming_fsm.api.SearchSpaceItem;


/**
 * Algorithm core
 */
public class FrequentPatternMining {

  public static final String READER = "R";
  public static final String GROWER = "G";
  public static final String AGGREGATOR = "A";
  public static final String COLLECTOR = "4";

  ArrayList<SearchSpaceItem> input = new ArrayList<>();

  TopologyBuilder topology;

  ResultHolder resultHolder;

  // number of grower bolts
    Integer numberOfGrowerBolts = 3;
    Integer numberOfAggregatorInstances = 3;

    float min_support = 0;

    public void setNumberOfAggregatorInstances(Integer numberOfAggregatorInstances) {
        this.numberOfAggregatorInstances = numberOfAggregatorInstances;
    }

    public void setNumberOfGrowerBolts(Integer numberOfGrowerBolts) {
        this.numberOfGrowerBolts = numberOfGrowerBolts;
    }

  public ResultHolder getResultHolder() {
    return resultHolder;
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
     * Setzt den benötigten min_support
     *
     * @param min_support
     */
    public void setMin_support(float min_support) {
        this.min_support = min_support;
    }




  /**
     * triggers mining process
     */
    public TopologyBuilder genTopology() {

      // set up Storm topology
      topology = new TopologyBuilder();

      // reader spout : 
      // reads source data and distributes items among grower bolts
      Reader reader = new Reader();
      reader.setData(input);

      topology
        .setSpout(READER, reader);

      // grower bolts:
      topology
          .setBolt(GROWER, new Grower(), this.numberOfGrowerBolts)
          // sent items randomly to growers
          .shuffleGrouping(READER, Reader.ITEM_STREAM)
          // set up grower input streams
          .allGrouping(READER, Reader.ITEM_DISTRIBUTION_FINISHED_STREAM)
          .allGrouping(AGGREGATOR, Aggregator.INFREQUENT_STREAM)
          .allGrouping(AGGREGATOR, Aggregator.FREQUENT_STREAM);

        // aggregators
        Aggregator aggregator = new Aggregator();
        aggregator.setNumberOfElements(input.size());
        aggregator.set_min_support(min_support);

        System.out.println("Support:" + min_support);
        topology
          .setBolt(AGGREGATOR, aggregator, this.numberOfAggregatorInstances)
          // same pattern sent to same aggregator
          .fieldsGrouping(GROWER, "element", new Fields("element"))
          // input stream from grower about phase
          .allGrouping(GROWER, Grower.PHASE);

        // collector
        Collector collector = new Collector();
        resultHolder = new ResultHolder();
        collector.setResult(resultHolder);
        collector.setNumberOfSplitterInstances(numberOfGrowerBolts);

        topology
          .setBolt(COLLECTOR, collector)
          .shuffleGrouping(AGGREGATOR, Aggregator.COLLECT_STREAM)
          .shuffleGrouping(GROWER, Grower.THREAD_DONE_STREAM);

        // https://issues.apache.org/jira/browse/FLINK-2836
        // zyklische Graphen sind in der Kompabilität nicht möglich

      return topology;
    }
}
