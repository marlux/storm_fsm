package streaming_fsm.interfaces;

import java.io.Serializable;

/**
 * Created by marlux on 12.04.16.
 */
public abstract class Pattern implements Serializable{

    SearchSpaceItem PatternOfItem;
    Embedding embedding;

    public abstract Integer size();
    public abstract int hashCode();

}
