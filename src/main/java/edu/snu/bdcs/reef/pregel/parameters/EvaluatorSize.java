package edu.snu.bdcs.reef.pregel.parameters;

/**
 * Created by puppybit on 14. 11. 13.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Memory size for each evaluator in MBs",
        short_name = "evalSize",
        default_value = "1024")
public final class EvaluatorSize implements Name<Integer> {
}
