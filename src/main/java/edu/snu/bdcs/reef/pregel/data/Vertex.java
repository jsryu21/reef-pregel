package edu.snu.bdcs.reef.pregel.data;

import org.apache.mahout.math.Vector;

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


    public Vertex(final Vector vector){
        this.vertexId = (int) vector.getElement(0).get();
        this.vertexValue = (double) vector.getElement(1).get();
        for (int i=2; i<vector.size();i++ )
        {
            this.adjVertex.add((int)vector.getElement(i).get());
        }

    }

    public final int getVertextId() {return vertexId;}
    public final double getVertextValue() {return vertexValue;}
    public final List<Integer> getAdjVertexList() {return adjVertex;}

}
