package edu.snu.bdcs.reef.pregel.parameters;


import edu.snu.bdcs.reef.pregel.utils.Parameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Created by puppybit on 14. 11. 18.
 */


public final class PregelParameters implements Parameters{

    private final int maxSuperSteps;

    @Inject
    private PregelParameters(@Parameter(MaxSuperSteps.class) final int maxSuperSteps){
        this.maxSuperSteps = maxSuperSteps;
    }

    public static void registerShortNameOfClass(CommandLine cl) {
        cl.registerShortNameOfClass(MaxSuperSteps.class);
    }

    @Override
    public Configuration getDriverConfiguration() {
        return Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(MaxSuperSteps.class, String.valueOf(maxSuperSteps))
                .build();
    }

    @Override
    public Configuration getCompTaskConfiguration() {
        return Tang.Factory.getTang().newConfigurationBuilder()
                .build();
    }

    @Override
    public Configuration getCtrlTaskConfiguration() {
        return Tang.Factory.getTang().newConfigurationBuilder()
                .bindNamedParameter(MaxSuperSteps.class, String.valueOf(maxSuperSteps))
                .build();
    }

}
