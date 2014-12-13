package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import edu.snu.bdcs.reef.pregel.data.PregelDataParser;
import edu.snu.bdcs.reef.pregel.groupcomm.names.InitialTopoReduce;
import edu.snu.bdcs.reef.pregel.groupcomm.subs.VertexListCodec;
import edu.snu.bdcs.reef.pregel.groupcomm.subs.VertexListReduceFunction;
import edu.snu.bdcs.reef.pregel.parameters.PregelParameters;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CommunicationGroup;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CtrlSyncBroadcast;
import edu.snu.bdcs.reef.pregel.utils.DataParseService;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for the Pregel REEF application.
 * This class is appropriate for setting up event handlers as well as configuring
 * Scatter and Reduce operations for Group Communication.
 */


public final class PregelDriver {
    private final static Logger LOG = Logger.getLogger(PregelDriver.class.getName());

    /**
     * Sub-id for Compute Tasks.
     * This object grants different IDs to each task
     * e.g. ComputeTask-0, ComputeTask-1, and so on.
     */
    private final AtomicInteger taskId = new AtomicInteger(0);

    /**
     * ID of the Context that goes under Controller Task.
     * This string is used to distinguish the Context that represents the Controller Task
     * from Contexts that go under Compute Tasks.
     */
    private String ctrlTaskContextId;


    /**
     * Object that sends requests to the resource manager
     */
    private final EvaluatorRequestor requestor;


    /**
     * Driver that manages Group Communication settings
     */
    private final GroupCommDriver groupCommDriver;

    /**
     *  Communication Group to work on
     */
    private final CommunicationGroupDriver commGroup;

    /**
     * Accessor for data loading service
     * Can check whether a evaluator is configured with the service or not.
     */
    private final DataLoadingService dataLoadingService;


    private final PregelParameters pregelParameters;

    /**
     * This class is instantiated by TANG
     *
     * Constructor for Driver of k-means job.
     * Store various objects as well as configuring Group Communication with
     * Broadcast and Reduce operations to use.
     *
     * @param requestor object used to request for new evaluators to the resource manager
     * @param groupCommDriver manager for group communication configurations
     * @param dataLoadingService manager for Data Loading configurations
     * @param pregelParameters parameter manager related specifically to the Pregel DSL
     */

    @Inject
    private PregelDriver(final EvaluatorRequestor requestor,
                         final GroupCommDriver groupCommDriver,
                         final DataLoadingService dataLoadingService,
                         final PregelParameters pregelParameters) {
        this.requestor = requestor;
        this.groupCommDriver = groupCommDriver;
        this.dataLoadingService = dataLoadingService;
        this.pregelParameters = pregelParameters;

        this.commGroup = groupCommDriver.newCommunicationGroup(
                CommunicationGroup.class,
                dataLoadingService.getNumberOfPartitions() + 1);

        this.commGroup
                .addBroadcast(CtrlSyncBroadcast.class,
                        BroadcastOperatorSpec.newBuilder()
                                .setSenderId(PregelControllerTask.TASK_ID)
                                .setDataCodecClass(SerializableCodec.class)
                                .build())
                .addReduce(InitialTopoReduce.class,
                        ReduceOperatorSpec.newBuilder()
                                .setReceiverId(PregelControllerTask.TASK_ID)
                                .setDataCodecClass(VertexListCodec.class)
                                .setReduceFunctionClass(VertexListReduceFunction.class)
                                .build())
                .finalise();

    }

    final class ActiveContextHandler implements org.apache.reef.wake.EventHandler<ActiveContext> {

        @Override
        public void onNext(final ActiveContext activeContext) {

            if (!groupCommDriver.isConfigured(activeContext)) {
                Configuration groupCommContextConf = groupCommDriver.getContextConfiguration();
                Configuration groupCommServiceConf = groupCommDriver.getContextConfiguration();
                Configuration finalServiceConf;

                if (dataLoadingService.isComputeContext(activeContext)) {

                    LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerTask to underlying context");
                    ctrlTaskContextId = getContextId(groupCommContextConf);
                    finalServiceConf = groupCommServiceConf;
                } else {
                    LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");
                    final Configuration dataParseConf = DataParseService.getServiceConfiguration(PregelDataParser.class);
                    finalServiceConf = Configurations.merge(groupCommServiceConf, dataParseConf);
                }

                activeContext.submitContextAndService(groupCommServiceConf, finalServiceConf);

            } else {
                final Configuration partialTaskConf;

                if (activeContext.getId().equals(ctrlTaskContextId)) {
                    LOG.log(Level.INFO, "Submit ControllerTask");
                    partialTaskConf = Configurations.merge(
                            TaskConfiguration.CONF
                                    .set(TaskConfiguration.IDENTIFIER, PregelControllerTask.TASK_ID)
                                    .set(TaskConfiguration.TASK, PregelControllerTask.class)
                                    .build(),
                            pregelParameters.getCtrlTaskConfiguration());
                } else {
                    LOG.log(Level.INFO, "Submit ComputeTask");
                    partialTaskConf = Configurations.merge(
                            TaskConfiguration.CONF
                                    .set(TaskConfiguration.IDENTIFIER, "CmpTask-" + taskId.getAndIncrement())
                                    .set(TaskConfiguration.TASK, PregelComputeTask.class)
                                    .build(),
                            pregelParameters.getCompTaskConfiguration());
                }

                commGroup.addTask(partialTaskConf);
                final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
                activeContext.submitContext(finalTaskConf);
            }
        }
    }

    /**
     * When a certain Compute Task fails, we add the Task back and let it participate in
     * Group Communication again. However if the failed Task is the Controller Task,
     * we just shut down the whole job because it's hard to recover the cluster centroid info.
     */

    final class FailedTaskHandler implements EventHandler<FailedTask>{
        @Override
        public void onNext (FailedTask failedTask) {
            LOG.info(failedTask.getId() + "has failed");

            //Stop the whole job if the failed Task is the Compute Task
            if (failedTask.getActiveContext().get().getId().equals(ctrlTaskContextId)) {
                throw new RuntimeException("Controller Task failed; aborting job");
            }

            final Configuration partialTaskConf = Tang.Factory.getTang()
                    .newConfigurationBuilder(
                            TaskConfiguration.CONF
                                    .set(TaskConfiguration.IDENTIFIER, failedTask.getId() + "-R")
                                    .set(TaskConfiguration.TASK, PregelComputeTask.class)
                                    .build())
                    .build();

            // Re-add the failed Compute Task
            commGroup.addTask(partialTaskConf);
            final Configuration taskConf = groupCommDriver.getTaskConfiguration((partialTaskConf));
            failedTask.getActiveContext().get().submitTask(taskConf);
        }

    }


    /**
     * Return the ID of the given Context
     */
    private String getContextId(final Configuration contextConf) {
        try {
            final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
            return injector.getNamedInstance(ContextIdentifier.class);
        } catch (final InjectionException e) {
            throw new RuntimeException("Unable to inject context identifier from context conf", e);
        }
    }
}
