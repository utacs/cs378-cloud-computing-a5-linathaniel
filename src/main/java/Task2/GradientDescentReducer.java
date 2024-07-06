// GradientDescentReducer.java
package Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GradientDescentReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private double learningRate = 0.001;
    private double m = 0.0;
    private double b = 0.0;
    private double totalLoss = 0.0;
    private long count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        m = Double.parseDouble(conf.get("m", "0.0"));
        b = Double.parseDouble(conf.get("b", "0.0"));
        learningRate = Double.parseDouble(conf.get("learningRate", "0.001"));
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (key.toString().equals("m_gradient")) {
            m -= learningRate * sum / count;
        } else if (key.toString().equals("b_gradient")) {
            b -= learningRate * sum / count;
        } else if (key.toString().equals("loss")) {
            totalLoss += sum;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("m"), new DoubleWritable(m));
        context.write(new Text("b"), new DoubleWritable(b));
        context.write(new Text("loss"), new DoubleWritable(totalLoss / count));
    }
}
