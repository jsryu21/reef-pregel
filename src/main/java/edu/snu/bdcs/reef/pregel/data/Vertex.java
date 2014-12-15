package edu.snu.bdcs.reef.pregel.data;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class Vertex implements Serializable{

    private final int vertexId;
    public Vector messageVector;


    public Vertex(final int vertexId, final Vector vector){
        this.vertexId = vertexId;
        this.messageVector = vector;
    }


    public Vertex(final Vector rawVector){
        this.vertexId = (int) rawVector.get(0);
        final Vector vector = new DenseVector(rawVector.size() - 1);
        for (int i = 1; i < rawVector.size(); i++){
            vector.set(i-1, rawVector.get(i));
        }
        this.messageVector = vector;
    }

    public final int getVertextId() {return this.vertexId;}
    public final double getVertextValue() {return this.messageVector.get(0);}
    public final Vector getAdjVector() {return this.messageVector;}

    @SuppressWarnings("boxing")
    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder("PregelVertex(");
        try (final Formatter formatter = new Formatter(b, Locale.US)) {
            formatter.format("Id %d, ", this.vertexId);
            for (int i = 0; i < this.messageVector.size() - 1; ++i) {
                formatter.format("%1.3f, ", this.messageVector.get(i));
            }
            formatter.format("%d", this.messageVector.get(this.messageVector.size() - 1));
        }
        b.append(')');
        return b.toString();
    }



}

