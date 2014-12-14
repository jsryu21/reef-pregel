package edu.snu.bdcs.reef.pregel.groupcomm.subs;

import edu.snu.bdcs.reef.pregel.Inject;
import edu.snu.bdcs.reef.pregel.data.Vertex;
import org.apache.reef.io.serialization.Codec;

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

        int adjVertexListSizeSum = 0;

        for (final Vertex vertex : list) {
            adjVertexListSizeSum += vertex.getAdjVertexList().size();
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE + Integer.SIZE * 2 * list.size()
        + Double.SIZE * list.size() + Integer.SIZE * adjVertexListSizeSum);

        try (final DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeInt(list.size());
            for (final Vertex vertex : list){
                daos.writeInt(vertex.getVertextId());
                daos.writeDouble(vertex.getVertextValue());
                daos.writeInt(vertex.adjVertexList.size());

                for (int j = 0; j < vertex.adjVertexList.size(); j++){
                    daos.writeInt(vertex.adjVertexList.get(j));
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
                final double vertexValue = dais.readDouble();
                final int vertexListSize = dais.readInt();
                final List<Integer> adjVertexList = new ArrayList<Integer>(vertexListSize);

                for (int j = 0; j < vertexListSize; j++) {
                    adjVertexList.add(j, dais.readInt());
                }

                list.add(new Vertex(vertexID, vertexValue, adjVertexList));
            }

        } catch (final IOException e){
            throw new RuntimeException(e.getCause());
        }


        return list;
    }
}
