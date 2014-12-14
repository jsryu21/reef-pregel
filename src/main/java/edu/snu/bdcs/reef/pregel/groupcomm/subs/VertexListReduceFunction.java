package edu.snu.bdcs.reef.pregel.groupcomm.subs;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.bdcs.reef.pregel.Inject;
import edu.snu.bdcs.reef.pregel.data.Vertex;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public class VertexListReduceFunction implements Reduce.ReduceFunction<List<Vertex>>{

    @Inject
    public VertexListReduceFunction(){

    }

    @Override
    public List<Vertex> apply(Iterable<List<Vertex>> elements) {
        final List<Vertex> resultList = new ArrayList<>();
        for (final List<Vertex> list : elements) {
            resultList.addAll(list);
        }


        return resultList;
    }
}
