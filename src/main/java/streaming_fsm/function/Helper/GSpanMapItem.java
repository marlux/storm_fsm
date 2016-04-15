package streaming_fsm.function.Helper;

import streaming_fsm.function.Helper.Enum.Frequent;
import streaming_fsm.api.Embedding;
import streaming_fsm.api.Pattern;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by thomasd on 13.04.16.
 */
public class GSpanMapItem {

    public Frequent frequent;
    public HashMap<Integer, ArrayList<Embedding>> embeddings;
    public Pattern parent = null;
    public ArrayList<Pattern> children;
    public Boolean done = false;

    public GSpanMapItem() {
        this.init();
    }

    public GSpanMapItem(GSpanMapItem gi) {
        this.frequent = gi.frequent;
        this.parent = gi.parent;

        this.embeddings = new HashMap<>(gi.embeddings);
        this.children = new ArrayList<>(gi.children);
    }

    public void init() {
        children = new ArrayList<>();
        embeddings = new HashMap<>();
        this.frequent = Frequent.UNKNOWN;
    }

}
