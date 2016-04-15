package streaming_fsm;

import java.util.*;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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

  ArrayList<Pattern> result = new ArrayList<>();
    ArrayList<SearchSpaceItem> input = new ArrayList<>();

  /**
   * maximum lifetime of local storm cluster in milliseconds
   */
  Integer maxExecutionTime = 10000;
    Integer loopExecutionTime = 1000;
    Integer currentExecutionTime = 0;

  // number of grower bolts
    Integer numberOfGrowerBolts = 3;
    Integer numberOfAggregatorInstances = 3;

    float min_support = 0;

    public void setNumberOfAggregatorInstances(Integer numberOfAggregatorInstances) {
        this.numberOfAggregatorInstances = numberOfAggregatorInstances;
    }

    public void setMaxExecutionTime(Integer maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    public void setNumberOfGrowerBolts(Integer numberOfGrowerBolts) {
        this.numberOfGrowerBolts = numberOfGrowerBolts;
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
     * triggers mining process
     */
    public void compute() {
      // set up Storm topology
        TopologyBuilder builder = new TopologyBuilder();

      // reader spout : 
      // reads source data and distributes items among grower bolts
      Reader reader = new Reader();
      reader.setData(input);

      builder.setSpout(READER, reader);

      // grower bolts:

      builder
          .setBolt(GROWER, new Grower(), this.numberOfGrowerBolts)
          // sent items randomly to growers
          .shuffleGrouping(READER, Reader.ITEM_STREAM)
          // set up grower input streams
          .allGrouping(READER, Reader.PHASE_STREAM)
          .allGrouping(AGGREGATOR, Aggregator.INFREQUENT_STREAM)
          .allGrouping(AGGREGATOR, Aggregator.FREQUENT_STREAM);

        // aggregators
        Aggregator aggregator = new Aggregator();
        aggregator.setNumberOfElements(input.size());
        aggregator.set_min_support(min_support);

        System.out.println("Support:" + min_support);
        builder
          .setBolt(AGGREGATOR, aggregator, this.numberOfAggregatorInstances)
          // same pattern sent to same aggregator
          .fieldsGrouping(GROWER, "element", new Fields("element"))
          // input stream from grower about phase
          .allGrouping(GROWER, Grower.PHASE);

        // 4
        Collector collector = new Collector();
        ResultHolder er = new ResultHolder();
        collector.setResult(er);
        collector.setNumberOfSplitterInstances(numberOfGrowerBolts);

        builder.setBolt(COLLECTOR, collector).shuffleGrouping(AGGREGATOR, "frequent")
                .shuffleGrouping(GROWER, "done");

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
