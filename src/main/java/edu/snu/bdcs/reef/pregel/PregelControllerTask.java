package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CommunicationGroup;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CtrlSyncBroadcast;
import edu.snu.bdcs.reef.pregel.groupcomm.names.InitialTopoReduce;
import edu.snu.bdcs.reef.pregel.parameters.ControlMessage;
import org.apache.reef.task.Task;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class PregelControllerTask implements Task{

    private static final Logger LOG = Logger.getLogger(PregelControllerTask.class.getName());

    /**
     * Task ID used for configuring Group Communication
     */
    public static final String TASK_ID = "ControllerTask";

    /**
     * Accessor for retrieving communicator members for Group Communication
     */
    private final CommunicationGroupClient communicationGroupClient;


    /**
     * Receive initial Graph topology from Compute Tasks
     */

    private final Reduce.Receiver<List<Vertex>> initialTopologyReduce;

    /**
     * Send control messages to Compute Tasks on what to do
     * e.g. TERMINATE, COMPUTE
     */

    private final Broadcast.Sender<ControlMessage> ctrlMsgBroadcast;

    /**
     * This class is instantiated by TANG
     *
     * Constructs the Controller Task for k-means
     *
     * @param groupCommClient accessor for retrieving communicator members for Group Communication
     */


    @Inject
    public PregelControllerTask(final GroupCommClient groupCommClient){

        super();

        this.communicationGroupClient = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
        this.initialTopologyReduce = communicationGroupClient.getReduceReceiver(InitialTopoReduce.class);
        this.ctrlMsgBroadcast = communicationGroupClient.getBroadcastSender(CtrlSyncBroadcast.class);

    }



    @Override
    public final byte[] call(byte[] memento) throws Exception {
        LOG.log(Level.INFO, "ControllerTask.call() commencing....");




        return null;
    }
}