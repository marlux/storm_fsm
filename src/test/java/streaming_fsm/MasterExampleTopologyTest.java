package streaming_fsm;

import org.junit.Test;
import streaming_fsm.InterfaceImplementations.Sequence;
import streaming_fsm.InterfaceImplementations.SubSequence;
import streaming_fsm.interfaces.Pattern;
import streaming_fsm.interfaces.SearchSpaceItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by marlux on 06.01.16.
 */
public class MasterExampleTopologyTest {


    @Test
    public void testMain() throws Exception {
        ArrayList<SearchSpaceItem> searchSpace = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            searchSpace.add(new Sequence(new Integer[]{1, 2, 3, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{2, 3, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{1, 4, 5}));
            searchSpace.add(new Sequence(new Integer[]{1, 2, 3, 4}));
        }


        ArrayList<Pattern> expectedResult = new ArrayList<>();
        expectedResult.add(new SubSequence(new Integer[]{1}));
        expectedResult.add(new SubSequence(new Integer[]{2}));
        expectedResult.add(new SubSequence(new Integer[]{2, 3}));
        expectedResult.add(new SubSequence(new Integer[]{2, 3, 4}));
        expectedResult.add(new SubSequence(new Integer[]{3}));
        expectedResult.add(new SubSequence(new Integer[]{3, 4}));
        expectedResult.add(new SubSequence(new Integer[]{4}));
        expectedResult.add(new SubSequence(new Integer[]{4, 5}));
        expectedResult.add(new SubSequence(new Integer[]{5}));


        // Hier muss das ergebnis erstellt werden

        SeqStreamer seqStream = new SeqStreamer();
        seqStream.setInput(searchSpace);
        seqStream.setMin_support(0.75f);
        seqStream.setMaxExecutionTime(500000);
        seqStream.compute();

        List<Pattern> result = seqStream.getResult();

        assertTrue(result.size() == expectedResult.size());

        Map<Pattern, Pattern> map = new HashMap<>();

        for (Pattern expaction : expectedResult) {
            for (Pattern resultLine : result) {
                if (expaction.equals(resultLine) &&
                        !map.containsKey(expaction) &&
                        !map.containsValue(resultLine)) {
                    map.put(expaction, resultLine);
                }

            }
        }
    }
}
