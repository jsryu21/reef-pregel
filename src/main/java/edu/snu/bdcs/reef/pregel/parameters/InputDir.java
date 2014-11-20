package edu.snu.bdcs.reef.pregel.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Created by puppybit on 14. 11. 13.
 */
@NamedParameter(doc = "File or directory to read input data from",
        short_name = "input")
public final class InputDir implements Name<String> {
}
