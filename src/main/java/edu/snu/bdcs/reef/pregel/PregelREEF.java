package edu.snu.bdcs.reef.pregel;

import com.microsoft.reef.io.network.nggroup.impl.driver.GroupCommService;
import edu.snu.bdcs.reef.pregel.parameters.*;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by puppybit on 14. 11. 13.
 */
public final class PregelREEF {

    private static final Logger LOG = Logger.getLogger(PregelREEF.class.getName());

    private final int evalSize;
    private final String inputDir;
    private final boolean onLocal;
    private final int splitNum;
    private final int timeout;
    private final PregelParameters pregelParameters;

    /**
     * This class is instantiated by TANG
     *
     * Create a job launcher configured with input command line arguments
     *
     * @param evalSize size of evaluators to use
     * @param inputDir directory or file of input data
     * @param onLocal whether to run on local or yarn runtime
     * @param splitNum desired number of splits to read data
     * @param timeout maximum time allowed for the job to run, in milliseconds
     * @param pregelParameters parameters specific to the pregel framework
     */

    @Inject
    private PregelREEF(@Parameter(EvaluatorSize.class) final int evalSize,
                       @Parameter(InputDir.class) final String inputDir,
                       @Parameter(OnLocal.class) final boolean onLocal,
                       @Parameter(SplitNum.class) final int splitNum,
                       @Parameter(Timeout.class) final int timeout,
                       final PregelParameters pregelParameters) {
        this.evalSize = evalSize;
        this.inputDir = inputDir;
        this.onLocal = onLocal;
        this.splitNum = splitNum;
        this.timeout = timeout;
        this.pregelParameters = pregelParameters;
    }

    private final static PregelREEF parseCommandLine(final String[] args) throws IOException, InjectionException {
        final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
        final CommandLine cl = new CommandLine(cb);

        cl.registerShortNameOfClass(EvaluatorSize.class);
        cl.registerShortNameOfClass(InputDir.class);
        cl.registerShortNameOfClass(OnLocal.class);
        cl.registerShortNameOfClass(SplitNum.class);
        cl.registerShortNameOfClass(Timeout.class);

        cl.processCommandLine(args);

        return Tang.Factory.getTang().newInjector(cb.build()).getInstance(PregelREEF.class);
    }
    public final static void main(final String[] args) {
        LauncherStatus status;
        try {
            PregelREEF pregelREEF = parseCommandLine(args);
            status = pregelREEF.run();

        } catch (final Exception e) {
            LOG.log(Level.SEVERE, "Fatal exception occurred.");
            status = LauncherStatus.FAILED(e);
        }

        LOG.log(Level.INFO, "REEF job completed: {0}", status);
    }

    private final LauncherStatus run() throws InjectionException{
        return DriverLauncher.getLauncher(getRuntimeConfiguration())
                .run(getDriverConfWithDataLoad(), timeout);
    }

    private final Configuration getRuntimeConfiguration() {
        return onLocal? getLocalRuntimeConfiguration() : getYarnRuntimeConfiguration();
    }

    private final Configuration getYarnRuntimeConfiguration() {
        return YarnClientConfiguration.CONF.build();
    }


    private final Configuration getLocalRuntimeConfiguration() {
        return LocalRuntimeConfiguration.CONF
                .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, splitNum + 1)
                .build();
    }


    private final Configuration getDriverConfWithDataLoad() {
        final ConfigurationModule pregelDriverConf = DriverConfiguration.CONF
                .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(PregelDriver.class))
                .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TextInputFormat.class))
                .set(DriverConfiguration.DRIVER_IDENTIFIER, "Pregel")
                .set(DriverConfiguration.ON_CONTEXT_ACTIVE, PregelDriver.ActiveContextHandler.class)
                .set(DriverConfiguration.ON_TASK_FAILED, PregelDriver.FailedTaskHandler.class);

        final EvaluatorRequest evalRequest = EvaluatorRequest.newBuilder()
                .setNumber(1)
                .setMemory(evalSize)
                .build();

        final Configuration pregelDriverConfwithDataLoad = new DataLoadingRequestBuilder()
                .setMemoryMB(evalSize)
                .setInputFormatClass(TextInputFormat.class)
                .setInputPath(inputDir)
                .setNumberOfDesiredSplits(splitNum)
                .setComputeRequest(evalRequest)
                .setDriverConfigurationModule(pregelDriverConf)
                .build();

        return Configurations.merge(pregelDriverConfwithDataLoad,
                GroupCommService.getConfiguration(),
                pregelParameters.getDriverConfiguration());

    }

}
