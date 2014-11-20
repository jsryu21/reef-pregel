package edu.snu.bdcs.reef.pregel.groupcomm.names;

/**
 * Created by puppybit on 14. 11. 18.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Each vertex transmit its value to neighborhood nodes with MsgBroadcast")
public final class MsgBroadcast implements Name<String> {
}
