package edu.snu.bdcs.reef.pregel.parameters;

/**
 * Created by puppybit on 14. 11. 13.
 */
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Number of splits to read data in",
        short_name = "split",
        default_value = "4")
public final class SplitNum implements Name<Integer> {
}
