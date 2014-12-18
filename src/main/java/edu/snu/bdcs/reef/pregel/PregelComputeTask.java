package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import edu.snu.bdcs.reef.pregel.groupcomm.names.*;
import edu.snu.bdcs.reef.pregel.parameters.ControlMessage;
import edu.snu.bdcs.reef.pregel.utils.DataParser;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.task.Task;

import javax.inject.Inject;
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
     * Send the message information to Controller Task
     */
    private final Reduce.Sender<List<Vector>> messageVectorReduce;


    /**
     * Receive control messages from Controller Task on what to do
     * e.g. INITIATE, TERMINATE, COMPUTE
     */

    private final Broadcast.Receiver<ControlMessage> ctrlSyncBroadcast;


    /**
     * receive the message information from Controller Task
     */

    private final Broadcast.Receiver<List<Vector>> messageVectorBroadcast;


    /**
     *
     * vertex read from input data
     *
     */
    private List<Vector> GroupList = new ArrayList<>();

    private List<Vector> ComputeList = new ArrayList<>();

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

    CommunicationGroupClient commGroupClient = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
    this.ctrlSyncBroadcast = commGroupClient.getBroadcastReceiver(CtrlSyncBroadcast.class);
    this.messageVectorReduce = commGroupClient.getReduceSender(MessageVectorReduce.class);
    this.messageVectorBroadcast = commGroupClient.getBroadcastReceiver(MessageVectorBroadcast.class);

}


    @Override
    public final byte[] call(final byte[] memento) throws Exception {

        LOG.log(Level.INFO, "ComputeTask.call() commencing....");

        //0. Read the vertex and edge from input data

        GroupList = dataParser.get().first;


        // 1. Start Algorithm

        boolean terminate = false;
        while (!terminate){
            switch (ctrlSyncBroadcast.receive()) {
                case TERMINATE:
                    terminate = true;
                    break;

                case INITIATE:
                    messageVectorReduce.send(GroupList);
                    break;

                case READY:
                    ComputeList = messageVectorBroadcast.receive();
                    for (int i = 0; i < GroupList.size();i++){
                        GroupList.get(i).set(1, ComputeList.get(0).get(1));
                    }

                    LOG.log(Level.SEVERE, "Debug2 " + "Node " + GroupList.get(0).get(0) + " Value : " + GroupList.get(0).get(1));

                case COMPUTE:

                    computePageRank();
                    LOG.log(Level.SEVERE, "Debug4");
                    break;

                default:
                    break;
            }
        }

        for (Vector resultVertex : GroupList){
            System.out.print("R_E_S_U_L_T : " + "Vertex " + (int)resultVertex.get(0) + ", Page Rank : " + Double.parseDouble(String.format("%.3f", resultVertex.get(1))) );
            System.out.printf("\n");
//            LOG.log(Level.SEVERE, "***Result : " + "Vertex Node " + (int)resultVertex.get(0) + " Page Rank : " + resultVertex.get(1));
        }

        return null;
    }

    private final void computePageRank() throws Exception{

        for (Vector computeVertex : GroupList) {

            double sum = 0;

            for (int i=0; i<ComputeList.size();i++){
                for (int j=2; j<ComputeList.get(i).size();j++){
                    if (computeVertex.get(0) == ComputeList.get(i).get(j)){
                        sum = sum + ComputeList.get(i).get(1);
                    }
                }
            }
            double mutableValue = 0.15 * computeVertex.get(1) + 0.85 * sum;
            computeVertex.set(1, mutableValue);
        }

        messageVectorReduce.send(GroupList);

    }

}
