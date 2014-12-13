package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import com.microsoft.reef.io.network.nggroup.impl.GroupChangesImpl;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CtrlSyncBroadcast;
import edu.snu.bdcs.reef.pregel.groupcomm.names.InitialTopoReduce;
import edu.snu.bdcs.reef.pregel.parameters.ControlMessage;
import edu.snu.bdcs.reef.pregel.utils.DataParser;
import edu.snu.cms.reef.ml.kmeans.groupcomm.names.CommunicationGroup;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
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


    /**
     *
     * vertex read from input data
     *
     */
    private List<Vector> vectorList = new ArrayList<>();

    private List<Vertex> vectexList = new ArrayList<>();

    /**
     * This class is instantiated by TANG
     *
     * Constructs a single Compute Task for k-means
     *
     * @param dataParser object that parses input data and returns it by the appropriate data structure
     * @param groupCommClient accessor for retrieving communicator members for Group Communication
     */

@Inject
PregelComputeTask(final DataParser<Pair<List<Vector>, List<Vector>>> dataParser,
                  final GroupCommClient groupCommClient){

    super();
    this.dataParser = dataParser;

    CommunicationGroupClient commCroupClient = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
    this.initialTopoReduce = commCroupClient.getReduceSender(InitialTopoReduce.class);
    this.ctrlSyncBroadcast = commCroupClient.getBroadcastReceiver(CtrlSyncBroadcast.class);

}


    @Override
    public final byte[] call(final byte[] memento) throws Exception {

        LOG.log(Level.INFO, "ComputeTask.call() commencing....");

        //0. Read the vertex and edge from input data

        vectorList = dataParser.get().first;

        for (final Vector vector : vectorList){
            vectexList.add(new Vertex(vector));
        }

        // 1. Start Algorithm





        return null;
    }
}
