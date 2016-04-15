package streaming_fsm.api;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by marlux on 12.04.16.
 */
public abstract class SearchSpaceItem implements Serializable {

    public abstract int size();

    public abstract Pattern genPattern(int i, int size);

    public abstract ArrayList<Embedding> getEmbedding(Pattern P);

    public abstract ArrayList<Pattern> grow(
      Pattern p, ArrayList<Embedding> embeddings);

}