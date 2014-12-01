package edu.snu.bdcs.reef.pregel.groupcomm.subs;

import edu.snu.bdcs.reef.pregel.data.Vertex;
import org.apache.reef.io.serialization.Codec;

import java.util.List;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class VertexListCodec implements Codec<List<Vertex>>{


    @Override
    public byte[] encode(List<Vertex> vertexes) {
        return new byte[0];
    }

    @Override
    public List<Vertex> decode(byte[] bytes) {
        return null;
    }
}
