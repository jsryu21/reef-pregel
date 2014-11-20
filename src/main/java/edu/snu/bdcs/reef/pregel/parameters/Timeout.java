package edu.snu.bdcs.reef.pregel.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by puppybit on 14. 11. 13.
 */

@NamedParameter(doc = "maximum time allowed for the job to run, in milliseconds",
        short_name = "timeout",
        default_value = "100000")
public final class Timeout implements Name<Integer> {
}
