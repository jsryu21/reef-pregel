package edu.snu.bdcs.reef.pregel.data;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class Vertex implements Serializable{

    private final int vertexId;
    public double vertexValue;
    public List<Integer> adjVertexList;


    public Vertex(final int vertexId, final double vertexValue, List<Integer> vertexList){
        this.vertexId = vertexId;
        this.vertexValue = vertexValue;
        this.adjVertexList = vertexList;
    }


    public Vertex(final Vector vector){
        this.vertexId = (int) vector.get(0);
        this.vertexValue = (double) vector.get(1);

        for (int i=2; i<vector.size();i++){
            this.adjVertexList.add((int) vector.get(i));
        }
    }

    public final int getVertextId() {return this.vertexId;}
    public final double getVertextValue() {return this.vertexValue;}
    public final List<Integer> getAdjVertexList() {return this.adjVertexList;}
}
