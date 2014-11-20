package edu.snu.bdcs.reef.pregel.groupcomm.names;

/**
 * Created by puppybit on 14. 11. 18.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name of the single communication group used for Pregel partitioning")
public final class CommunicationGroup implements Name<String> {
}