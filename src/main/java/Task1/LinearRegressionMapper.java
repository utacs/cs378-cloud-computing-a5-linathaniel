package Task1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinearRegressionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final Text X_KEY = new Text("X");
    private static final Text Y_KEY = new Text("Y");
    private static final Text XY_KEY = new Text("XY");
    private static final Text XX_KEY = new Text("XX");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length != 17) return;

        double tripDistance = Double.parseDouble(fields[5]);
        double fareAmount = Double.parseDouble(fields[11]);

        context.write(X_KEY, new DoubleWritable(tripDistance));
        context.write(Y_KEY, new DoubleWritable(fareAmount));
        context.write(XY_KEY, new DoubleWritable(tripDistance * fareAmount));
        context.write(XX_KEY, new DoubleWritable(tripDistance * tripDistance));
    }
}
