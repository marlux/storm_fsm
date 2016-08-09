package streaming_fsm.function.Environment;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import streaming_fsm.api.Pattern;
import streaming_fsm.function.Helper.ResultHolder;

import java.util.ArrayList;

/**
 * Created by marlux on 09.08.16.
 */
public class StormStarter {

    Config config;

    /**
    * maximum lifetime of local storm cluster in milliseconds
    */
    Integer maxExecutionTime = 10000;
    Integer loopExecutionTime = 1000;
    Integer currentExecutionTime = 0;


    // Switch for Cluster or Local compution
    boolean localComputition = true;


    public void setConfig(Config config) {
        this.config = config;
    }

    public void setLocalComputition(boolean localComputition) {
        this.localComputition = localComputition;
    }

    public void setMaxExecutionTime(Integer maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    ArrayList<Pattern> result;

    public ArrayList<Pattern> StartTopologieOnStorm(ResultHolder resultHolder, TopologyBuilder topology) {

        if(config == null) {
            System.out.println("Config is not set");
            return null;
        }

        if(this.localComputition) {
            LocalCluster cluster = new LocalCluster();
            // create topology on cluster
            try {
                cluster.submitTopology("Async Sequence Computioner", config, topology.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }

            while(!resultHolder.done && maxExecutionTime > currentExecutionTime) {
                Utils.sleep(loopExecutionTime);
                currentExecutionTime += loopExecutionTime;
            }

            if(currentExecutionTime >= maxExecutionTime && !resultHolder.done) {
                System.out.println("MAX EXECUTION TIME REACHED BUT NOT DONE");
                cluster.shutdown();
                System.exit(0);
            }

            this.result = resultHolder.getResult();

            System.out.println("Size: " + this.result.size());

            cluster.shutdown();

            return this.result;
        }
        else {

            // Hier muss noch der Cluster definiert werden --> http://storm.apache.org/releases/current/Running-topologies-on-a-production-cluster.html

            try {
                StormSubmitter.submitTopology("mytopology", config, topology.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }

            // Hier muss das ganze noch ausgewertet werden

            return this.result;
        }

    }
}
