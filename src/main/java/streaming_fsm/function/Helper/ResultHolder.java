package streaming_fsm.function.Helper;

import streaming_fsm.api.Pattern;

import java.io.Serializable;
import java.util.ArrayList;


/**
 * Created by marlux on 08.01.16.
 */
public class ResultHolder implements Serializable {

    ArrayList<Pattern> result;

    // TODO no public fields!!!
    public boolean done = false;

    public ResultHolder() {
        this.result = new ArrayList<>();
    }

    public void add(Pattern i) {
        if (!result.contains(i)) {
            result.add(i);
        }

    }

    public ArrayList<Pattern> getResult() {
        return result;
    }
}
