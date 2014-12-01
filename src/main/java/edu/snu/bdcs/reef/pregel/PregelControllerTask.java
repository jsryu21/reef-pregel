package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import org.apache.reef.task.Task;

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

    @Override
    public byte[] call(byte[] bytes) throws Exception {
        return new byte[0];
    }
}
