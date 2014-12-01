package edu.snu.bdcs.reef.pregel.parameters;

import java.io.Serializable;

public enum ControlMessage implements Serializable {
    TERMINATE, COMPUTE, INITIATE
}