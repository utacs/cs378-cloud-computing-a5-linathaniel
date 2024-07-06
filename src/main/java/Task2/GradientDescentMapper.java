// GradientDescentMapper.java
package Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GradientDescentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private double m = 0.0;
    private double b = 0.0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        m = Double.parseDouble(conf.get("m", "0.0"));
        b = Double.parseDouble(conf.get("b", "0.0"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        double x = Double.parseDouble(fields[5]);
        double y = Double.parseDouble(fields[11]);

        // Compute gradients
        double y_pred = m * x + b;
        double error = y - y_pred;

        context.write(new Text("m_gradient"), new DoubleWritable(-2 * x * error));
        context.write(new Text("b_gradient"), new DoubleWritable(-2 * error));
        context.write(new Text("loss"), new DoubleWritable(error * error));
    }
}
