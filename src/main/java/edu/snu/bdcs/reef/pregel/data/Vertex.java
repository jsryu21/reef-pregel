package edu.snu.bdcs.reef.pregel.data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class Vertex implements Serializable{

    private final int vertexId;
    public double vertexValue;
    public List<Integer> adjVertex;

    public Vertex(final int vertexId, final double vertexValue, final List<Integer> adjVertex){
        this.vertexId = vertexId;
        this.vertexValue = vertexValue;
        this.adjVertex = adjVertex;
    }

    public final int getVertextId() {return vertexId;}
    public final double getVertextValue() {return vertexValue;}
    public final List<Integer> getAdjVertexList() {return adjVertex;}

}
