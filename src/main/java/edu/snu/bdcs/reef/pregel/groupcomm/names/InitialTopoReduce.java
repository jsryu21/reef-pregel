package edu.snu.bdcs.reef.pregel.groupcomm.names;

/**
 * Created by puppybit on 14. 11. 18.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Initially Graph Topology information must submit to ControlTask")
public final class InitialTopoReduce implements Name<String> {
}