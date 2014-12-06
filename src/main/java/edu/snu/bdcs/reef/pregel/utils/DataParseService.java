package edu.snu.bdcs.reef.pregel.utils;

/**
 * Created by puppybit on 14. 12. 6.
 */

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A simple parse class given in the form of a service.
 * Should be inserted alongside a context.
 *
 * This class doesn't imply HOW data should be parsed; it just provides a
 * small interface for retrieving input data.
 *
 * The actual parse function needs to be given as an argument.
 */
@Unit
public final class DataParseService {
    private static Logger LOG = Logger.getLogger(DataParseService.class.getName());

    /**
     * parse function to exploit
     */
    private final DataParser dataParser;

    /**
     * This class is instantiated by TANG
     *
     * Constructor for parse manager, which accepts an actual parse function as a parameter
     * @param dataParser parse function to exploit
     */
    @Inject
    private DataParseService(DataParser dataParser) {
        this.dataParser = dataParser;
    }

    public static Configuration getServiceConfiguration(Class<? extends DataParser> impl) {
        Configuration partialServiceConf = ServiceConfiguration.CONF
                .set(ServiceConfiguration.SERVICES, impl)
                .set(ServiceConfiguration.ON_CONTEXT_STARTED, ContextStartHandler.class)
                .build();

        return Tang.Factory.getTang()
                .newConfigurationBuilder(partialServiceConf)
                .bindImplementation(DataParser.class, impl)
                .build();
    }

    private final class ContextStartHandler implements EventHandler<ContextStart> {
        @Override
        public void onNext(ContextStart contextStart) {
            LOG.log(Level.INFO, "Context started, asking parser to parse.");
            dataParser.parse();
        }
    }
}
