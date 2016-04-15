package streaming_fsm.InterfaceImplementations;

import org.apache.commons.lang3.ArrayUtils;
import streaming_fsm.interfaces.Pattern;

import java.util.Arrays;

/**
 * Created by marlux on 12.04.16.
 */
public class SubSequence extends Pattern {

    public Integer[] seq;

    public SubSequence(Integer[] seq) {
        this.seq = ArrayUtils.clone(seq);
    }

    public boolean equals(Object object2) {
        return ArrayUtils.isEquals(this.seq, ((SubSequence) object2).seq);
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
