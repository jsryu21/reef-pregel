package edu.snu.bdcs.reef.pregel.groupcomm.subs;

/**
 * Created by puppybit on 14. 12. 18.
 */
import com.microsoft.reef.io.network.group.operators.Reduce;
import org.apache.mahout.math.Vector;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Reduce function for simply gathering all Vector Lists sent into one single List
 */
public final class VectorListReduceFunction implements Reduce.ReduceFunction<List<Vector>> {

    @Inject
    public VectorListReduceFunction() {
    }

    @Override
    public List<Vector> apply(Iterable<List<Vector>> elements) {
        final List<Vector> resultList = new ArrayList<>();
        for (final List<Vector> list : elements) {
            resultList.addAll(list);
        }

        return resultList;
    }
}