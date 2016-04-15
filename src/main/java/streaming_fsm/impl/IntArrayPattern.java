package streaming_fsm.impl;

import org.apache.commons.lang3.ArrayUtils;
import streaming_fsm.api.Pattern;

import java.util.Arrays;

/**
 * Created by marlux on 12.04.16.
 */
public class IntArrayPattern extends Pattern {

    public Integer[] seq;

    public IntArrayPattern(Integer[] seq) {
        this.seq = ArrayUtils.clone(seq);
    }

    @Override
    public boolean equals(Object object2) {
        return ArrayUtils.isEquals(this.seq, ((IntArrayPattern) object2).seq);
    }

    @Override
    public Integer size() {
        return seq.length;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(seq);
    }
}
