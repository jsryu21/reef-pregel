package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import edu.snu.bdcs.reef.pregel.parameters.ControlMessage;
import edu.snu.cms.reef.ml.kmeans.utils.DataParser;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.task.Task;

import java.util.List;
import java.util.logging.Logger;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class PregelComputeTask implements Task{
    private static final Logger LOG = Logger.getLogger(PregelComputeTask.class.getName());


    /**
     * Parser object for input data that returns data assigned to this Task
     */

    private final DataParser<Pair<List<Vector>, List<Vector>>> dataParser;

    /**
     * Send the initial graph topology information to Controller Task
     * This is just test function
     */

    private  final Reduce.Sender<List<Vertex>> initialTopoReduce;

    /**
     * Receive control messages from Controller Task on what to do
     * e.g. INITIATE, TERMINATE, COnMPUTE
     */

    private final Broadcast.Receiver<ControlMessage> ctrlSyncBroadcast;



    @Override
    public byte[] call(byte[] bytes) throws Exception {
        return new byte[0];
    }
}
