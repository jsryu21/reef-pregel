package edu.snu.bdcs.reef.pregel.groupcomm.names;

/**
 * Created by puppybit on 14. 12. 19.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name for the operation used to collect Pregel Message at each SuperSteps")
public final class MessageVectorBroadcast implements Name<String> {
}