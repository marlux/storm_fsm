package streaming_fsm.function.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import streaming_fsm.function.Helper.Enum.Frequent;
import streaming_fsm.function.Helper.GSpanMapItem;
import streaming_fsm.api.Embedding;
import streaming_fsm.api.Pattern;
import streaming_fsm.api.SearchSpaceItem;
import streaming_fsm.function.Spout.Reader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by marlux on 06.01.16.
 */
public class Grower extends BaseRichBolt implements Runnable {

    public static final String PHASE = "phase";
    public static final String THREAD_DONE_STREAM = "done";

    OutputCollector outputCollector;

  /**
   * concurrent cause of 2 threads
   */
  public ConcurrentHashMap<Integer, SearchSpaceItem> Seq = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Pattern, GSpanMapItem> GspanMap = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        switch (tuple.getSourceStreamId()) {

            case Reader.ITEM_DISTRIBUTION_FINISHED_STREAM:
                // Starte eine Paralelle Berechnung
                // Es sind nun alle Daten verteilt
                System.out.println("Es wird der Thread gestartet");
                new Thread(this).start();
                break;

            case Reader.ITEM_STREAM:
                // Es werden die einzelnen Daten in den Bolt gespeichert

                Integer tupId = tuple.getIntegerByField(Reader.ITEM_ID_FIELD);
                SearchSpaceItem tupSeq = (SearchSpaceItem) tuple
                  .getValueByField(Reader.ITEM_DATA_FIELD);
                Seq.put(tupId, tupSeq);

                // Zerlege nun die Sequenz in alle Elemnte der Länge 1 und packe sie in die Liste

                for (int i = 0; i < tupSeq.size(); i++) {
                    Pattern pattern = tupSeq.genPattern(i, 1);

                    // Prüfe ob Pattern schon gefunden

                    if (GspanMap.containsKey(pattern)) {
                        GSpanMapItem gi = new GSpanMapItem(GspanMap.get(pattern));

                        if (!gi.embeddings.containsKey(tupId)) {
                            gi.embeddings.put(tupId, tupSeq.getEmbedding(pattern));

                            GspanMap.remove(pattern);
                            GspanMap.put(pattern, gi);
                        }
                    } else {
                        GSpanMapItem gi = new GSpanMapItem();
                        gi.embeddings.put(tupId, tupSeq.getEmbedding(pattern));
                        GspanMap.put(pattern, gi);
                    }
                }

                break;

            case Aggregator.INFREQUENT_STREAM:
                // Die Sequenz ist nicht frequent und wird aus der Datenbasis entfernt

                Stack<Pattern> p_infreq = new Stack<>();

                p_infreq.push((Pattern) tuple.getValueByField("seq"));

                while (p_infreq.size() > 0) {
                    Pattern current_p = p_infreq.pop();
                    GSpanMapItem gsmi = GspanMap.get(current_p);
                    if (gsmi != null) {
                        gsmi.done = true;
                        gsmi.frequent = Frequent.NO;
                        GspanMap.put(current_p,gsmi);
                        for (Pattern child : gsmi.children) {
                            p_infreq.push(child);
                        }
                        GspanMap.remove(current_p);
                    }
                }

                break;

            case Aggregator.FREQUENT_STREAM:
                // Die Sequenz ist frequent und wird vorzeitig gegrowt

                Pattern p = (Pattern) tuple.getValueByField("seq");
                GSpanMapItem gsmi = GspanMap.get(p);
                if (gsmi == null) break;

                if (!gsmi.done && gsmi.frequent == Frequent.UNKNOWN) {
                    computeCurrentGsmi(p, gsmi, false);
                }

                gsmi.done = true;
                gsmi.frequent = Frequent.YES;
                GspanMap.put(p, gsmi);
                break;
        }
        outputCollector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("element", new Fields("element", "seqId"));
        outputFieldsDeclarer.declareStream(PHASE, new Fields("phase", "size"));
        outputFieldsDeclarer.declareStream(THREAD_DONE_STREAM, new Fields("done"));

    }

    @Override
    public void run() {
        // Emit die ganzen einzelnen Sequenzen aus den Graphen
        // Zusätzlich schreibe wenn eine phase fertig ist


        int seqSize = 1;

        boolean somethingSendet = true;

        while (somethingSendet) {
            somethingSendet = false;

            Iterator it = GspanMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                Pattern p = (Pattern) pair.getKey();
                GSpanMapItem gsmi = (GSpanMapItem) pair.getValue();

                if (!gsmi.done && gsmi.frequent == Frequent.UNKNOWN) {

                    somethingSendet = computeCurrentGsmi(p, gsmi, somethingSendet);

                    gsmi.done = true;
                    GspanMap.put(p, gsmi);

                    // Prüfe hier ob noch etwas in der aktuellen Phase ist
                    Iterator checker = GspanMap.entrySet().iterator();

                    boolean breaker = false;
                    while (checker.hasNext()) {
                        Map.Entry checkerPair = (Map.Entry) checker.next();
                        Pattern key = (Pattern) checkerPair.getKey();
                        GSpanMapItem value = (GSpanMapItem) checkerPair.getValue();

                        if (!value.done && key.size() == seqSize) {
                            breaker = true;
                            break;
                        }
                    }

                    if (!breaker) {
                        this.outputCollector.emit(Reader.ITEM_DISTRIBUTION_FINISHED_STREAM, new Values
                          (seqSize,
                          Seq.size()));
                        seqSize++;
                    }

                }
            }
        }
        this.outputCollector.emit("done", new Values(1));
    }

    public boolean computeCurrentGsmi(Pattern p, GSpanMapItem gsmi, Boolean somethingSendet) {
        Iterator innerIt = gsmi.embeddings.entrySet().iterator();

        while (innerIt.hasNext()) {
            Map.Entry innerPair = (Map.Entry) innerIt.next();
            Integer key = (Integer) innerPair.getKey();
            ArrayList<Embedding> value = (ArrayList<Embedding>) innerPair.getValue();

            this.outputCollector.emit("element", new Values(p, key));
            somethingSendet = true;

            SearchSpaceItem ssi = Seq.get(key);
            ArrayList<Pattern> newPs = ssi.grow(p, value);

            addnewPattern(newPs, key, p);

            for (Pattern x : newPs) {
                if (!gsmi.children.contains(x))
                    gsmi.children.add(x);
            }

        }
        return somethingSendet;
    }

    public void addnewPattern(ArrayList<Pattern> newPs, Integer key, Pattern parent) {
        for (Pattern np : newPs) {
            GSpanMapItem gi;
            if (GspanMap.containsKey(np)) {
                gi = new GSpanMapItem(GspanMap.get(np));
            } else {
                gi = new GSpanMapItem();
            }

            gi.parent = parent;
            gi.embeddings.put(key, Seq.get(key).getEmbedding(np));
            GspanMap.put(np, gi);
        }
    }
}
