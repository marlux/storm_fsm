package streaming_fsm.InterfaceImplementations;

import org.apache.commons.lang3.ArrayUtils;

import streaming_fsm.interfaces.Embedding;
import streaming_fsm.interfaces.Pattern;
import streaming_fsm.interfaces.SearchSpaceItem;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by marlux on 12.04.16.
 */
public class Sequence extends SearchSpaceItem {

    public Integer[] seq;

    public Sequence(Integer[] seq) {
        this.seq = ArrayUtils.clone(seq);
    }

    @Override
    public int size() {
        return seq.length;
    }

    @Override
    public Pattern genPattern(int i, int size) {
        if (seq.length >= i + size) {
            return new SubSequence((new ArrayList<Integer>(Arrays.asList(seq)).subList(i, i + size)).toArray(new Integer[size]));
        }
        return null;
    }

    @Override
    public ArrayList<Embedding> getEmbedding(Pattern P) {

        ArrayList<Embedding> ret = new ArrayList<>();
        Integer l = P.size();

        for (int i = 0; i < seq.length - l + 1; i++) {
            Integer[] subarray = Arrays.copyOfRange(seq, i, i + l);
            if (ArrayUtils.isEquals(subarray, ((SubSequence) P).seq)) {
                SequenceEmbedding se = new SequenceEmbedding();
                se.pos = i;
                ret.add(se);
            }
        }
        return ret;
    }

    @Override
    public ArrayList<Pattern> grow(Pattern p, ArrayList<Embedding> embeddings) {
        Integer size = p.size() + 1;
        ArrayList<Pattern> ret = new ArrayList<>();

        for (Embedding e : embeddings) {
            Pattern ss = this.genPattern(((SequenceEmbedding) e).pos, size);

            if (ss != null) {
                ret.add(ss);
            }

        }
        return ret;
    }
}
