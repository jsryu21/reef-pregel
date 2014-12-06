package edu.snu.bdcs.reef.pregel.data;

import edu.snu.bdcs.reef.pregel.Inject;
import edu.snu.bdcs.reef.pregel.utils.DataParser;
import edu.snu.bdcs.reef.pregel.utils.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by puppybit on 14. 11. 30.
 */
public final class PregelDataParser implements DataParser<Pair<List<Vector>, List<Vector>>> {

    private final static Logger LOG = Logger.getLogger(PregelDataParser.class.getName());

    private final DataSet<LongWritable, Text> dataSet;
    private Pair<List<Vector>, List<Vector>> result;
    private ParseException parseException;

    @Inject
    public PregelDataParser(final DataSet<LongWritable, Text> dataSet) {this.dataSet = dataSet;}


    @Override
    public Pair<List<Vector>, List<Vector>> get() throws ParseException {

        if (result == null) {
            parse();
        }

        if (parseException != null){
            throw parseException;
        }

        return result;
    }

    @Override
    public void parse() {
        List<Vector> vertexList = new ArrayList<>();
        List<Vector> edgeList = new ArrayList<>();

        for (final Pair<LongWritable, Text> keyValue : dataSet) {
            String[] split = keyValue.toString().trim().split("\\s+");
            if (split.length == 0) {
                continue;
            }

            if (split[0].equals("*")) {
                final Vector vertex = new DenseVector(split.length - 1);
                try {
                    for (int i=1; i < split.length; i++) {
                        vertex.set(i-1, Double.valueOf(split[i]));
                    }
                    vertexList.add(vertex);
                } catch (final NumberFormatException e) {
                    parseException = new ParseException("Parse failed : numbers should be DOUBLE");
                    return;
                }
            } else if (split[0].equals("#")) {
                final Vector edge = new DenseVector(split.length - 1);
                try {
                    for (int i = 1; i < split.length; i++) {
                        edge.set(i-1, Double.valueOf(split[i]));
                    }
                    edgeList.add(edge);
                } catch (final NumberFormatException e) {
                    parseException = new ParseException("Parse failed : numbers should be DOUBLE");
                    return;
                }
            }

            result = new Pair<>(vertexList, edgeList);
        }
    }
}
