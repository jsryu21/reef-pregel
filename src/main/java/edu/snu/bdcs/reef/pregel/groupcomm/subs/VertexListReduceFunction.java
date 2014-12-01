package edu.snu.bdcs.reef.pregel.groupcomm.subs;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.bdcs.reef.pregel.data.Vertex;

import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public class VertexListReduceFunction implements Reduce.ReduceFunction<List<Vertex>>{
    @Override
    public List<Vertex> apply(Iterable<List<Vertex>> iterable) {
        return null;
    }
}
