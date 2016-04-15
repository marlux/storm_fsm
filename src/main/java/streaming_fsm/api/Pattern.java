package streaming_fsm.api;

import java.io.Serializable;

/**
 * Created by marlux on 12.04.16.
 */
public abstract class Pattern implements Serializable{
    public abstract Integer size();
    public abstract int hashCode();

}
