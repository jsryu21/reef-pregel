package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.group.operators.*;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import edu.snu.bdcs.reef.pregel.groupcomm.names.*;
import edu.snu.bdcs.reef.pregel.parameters.ControlMessage;
import edu.snu.bdcs.reef.pregel.parameters.MaxSuperSteps;
import org.apache.mahout.math.Vector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.ArrayList;
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

    private final Reduce.Receiver<List<Vector>> messageVectorReduce;


    /**
     * Send control messages to Compute Tasks on what to do
     * e.g. TERMINATE, COMPUTE
     */

    private final Broadcast.Sender<ControlMessage> ctrlMsgBroadcast;

    private final int maxSuperSteps;

    /**
     * This class is instantiated by TANG
     *
     * Constructs the Controller Task for k-means
     *
     * @param groupCommClient accessor for retrieving communicator members for Group Communication
     * @param maxSteps maximum number of superstpes
     */


    @Inject
    public PregelControllerTask(final GroupCommClient groupCommClient,
                                @Parameter(MaxSuperSteps.class) final int maxSteps){

        super();

        this.communicationGroupClient = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
        this.initialTopologyReduce = communicationGroupClient.getReduceReceiver(InitialTopoReduce.class);
        this.ctrlMsgBroadcast = communicationGroupClient.getBroadcastSender(CtrlSyncBroadcast.class);
        this.messageVectorReduce = communicationGroupClient.getReduceReceiver(MessageVectorReduce.class);
        this.maxSuperSteps = maxSteps;

    }



    @Override
    public final byte[] call(byte[] memento) throws Exception {
        LOG.log(Level.INFO, "ControllerTask.call() commencing....");

        ctrlMsgBroadcast.send(ControlMessage.INITIATE);

//        final List<Vertex> allVertexList = initialTopologyReduce.reduce();
        final List<Vector> vectorList = messageVectorReduce.reduce();

        LOG.log(Level.INFO, "Debug1 " + vectorList.get(0).get(0));


        ctrlMsgBroadcast.send(ControlMessage.TERMINATE);

        return null;
    }
}
