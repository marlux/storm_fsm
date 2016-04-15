package streaming_fsm;

import org.junit.Test;
import streaming_fsm.impl.Sequence;
import streaming_fsm.impl.IntArrayPattern;
import streaming_fsm.api.Pattern;
import streaming_fsm.api.SearchSpaceItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

public class MasterExampleTopologyTest {

    @Test
    public void testMain() throws Exception {
        // generate search space
        ArrayList<SearchSpaceItem> searchSpace = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            searchSpace.add(new Sequence(new Integer[]{1, 2, 3, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{2, 3, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{1, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{1, 2, 3, 4}));
        }

        // expected result
        ArrayList<Pattern> expectedResult = new ArrayList<>();
        expectedResult.add(new IntArrayPattern(new Integer[]{1}));
        expectedResult.add(new IntArrayPattern(new Integer[]{2}));
        expectedResult.add(new IntArrayPattern(new Integer[]{2, 3}));
        expectedResult.add(new IntArrayPattern(new Integer[]{2, 3, 4}));
        expectedResult.add(new IntArrayPattern(new Integer[]{3}));
        expectedResult.add(new IntArrayPattern(new Integer[]{3, 4}));
        expectedResult.add(new IntArrayPattern(new Integer[]{4}));
        expectedResult.add(new IntArrayPattern(new Integer[]{4, 5}));
        expectedResult.add(new IntArrayPattern(new Integer[]{5}));


        // Set up algorithm
        FrequentPatternMining miner = new FrequentPatternMining();
        miner.setInput(searchSpace);
        miner.setMin_support(0.75f);
        miner.setMaxExecutionTime(500000);

        // run algorithm
        miner.compute();
        List<Pattern> result = miner.getResult();

        // validate result
        assertTrue(result.size() == expectedResult.size());

        Map<Pattern, Pattern> map = new HashMap<>();

        for (Pattern expectation : expectedResult) {
            for (Pattern resultLine : result) {
                if (expectation.equals(resultLine) &&
                        !map.containsKey(expectation) &&
                        !map.containsValue(resultLine)) {
                    map.put(expectation, resultLine);
                }

            }
        }
    }
}
