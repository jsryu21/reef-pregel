package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import edu.snu.bdcs.reef.pregel.parameters.PregelParameters;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CommunicationGroup;
import edu.snu.bdcs.reef.pregel.groupcomm.names.MsgBroadcast;
import edu.snu.bdcs.reef.pregel.groupcomm.names.CtrlSyncBroadcast;
import edu.snu.bdcs.reef.pregel.groupcomm.names.InitialTopoBroadcast;
import javafx.event.EventHandler;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

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
     * @param dataLoadingService manager for Data Loading configurations
     * @param pregelParameters parameter manager related specifically to the Pregel DSL
     */

    @Inject
    private PregelDriver(final EvaluatorRequestor requestor,
                         final DataLoadingService dataLoadingService,
                         final PregelParameters pregelParameters) {
        this.requestor = requestor;
        this.groupCommDriver = groupCommDriver;
        this.dataLoadingService = dataLoadingService;
        this.pregelParameters = pregelParameters;

        this.commGroup = groupCommDriver.newCommunicationGroup(
                CommunicationGroup.class,
                dataLoadingService.getNumberOfPartitions() +1);

    }

    final class ActiveContextHandler implements EventHandler<ActiveContext>{

        @Override
        public void onNext(final ActiveContext activeContext) {


            if(dataLoadingService.isComputeContext(activeContext)) {
                LOG.log(Level.INFO, "Submitting DataLoadingContext for ControllerTask");
                Configuration dataLoadingContextConf = dataLoadingService.getContextConfiguration();
                Configuration dataLoadingServiceConf = dataLoadingService.getServiceConfiguration();
                ctrlTaskContextId = getContextId(dataLoadingContextConf);

            }


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
