package edu.snu.bdcs.reef.pregel.groupcomm.subs;

import edu.snu.bdcs.reef.pregel.data.Vertex;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;


import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class VertexListCodec implements Codec<List<Vertex>>{

    @Inject
    public VertexListCodec() {

    }

    @Override
    public final byte[] encode(final List<Vertex> list) {

        int adjVertexVectorSizeSum = 0;

        for (final Vertex vertex : list) {
            adjVertexVectorSizeSum += vertex.messageVector.size();
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE + Integer.SIZE * 2 * list.size()
        + Double.SIZE * adjVertexVectorSizeSum);

        try (final DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeInt(list.size());
            for (final Vertex vertex : list){
                daos.writeInt(vertex.getVertextId());
                daos.writeInt(vertex.messageVector.size());

                for (int j = 0; j < vertex.messageVector.size(); j++){
                    daos.writeDouble(vertex.messageVector.get(j));
                }
            }

        } catch (final IOException e) {
            throw new RuntimeException(e.getCause());
        }

        return baos.toByteArray();
    }

    @Override
    public List<Vertex> decode(final byte[] data) {
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        final List<Vertex> list = new ArrayList<>();

        try (final DataInputStream dais = new DataInputStream(bais)){
            final int listSize = dais.readInt();

            for (int i = 0; i < listSize; i++){
                final int vertexID = dais.readInt();
                final int messageVectorSize = dais.readInt();
                final Vector vector = new DenseVector(messageVectorSize);

                for (int j = 0; j < messageVectorSize; j++) {
                    vector.set(j, dais.readDouble());
                }

                list.add(new Vertex(vertexID, vector));
            }

        } catch (final IOException e){
            throw new RuntimeException(e.getCause());
        }


        return list;
    }
}
