package edu.snu.bdcs.reef.pregel.groupcomm.names;

/**
 * Created by puppybit on 14. 11. 18.
 */

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "All vertex operation is synchronized when they received CtrlSyncBroadcast from ControlTask")
public final class CtrlSyncBroadcast implements Name<String> {
}

