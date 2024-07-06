package Task1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LinearRegressionReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private double sumX = 0;
    private double sumY = 0;
    private double sumXY = 0;
    private double sumXX = 0;
    private long count = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            switch (key.toString()) {
                case "X":
                    sumX += value.get();
                    break;
                case "Y":
                    sumY += value.get();
                    break;
                case "XY":
                    sumXY += value.get();
                    break;
                case "XX":
                    sumXX += value.get();
                    break;
            }
            count++;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        double m = (count * sumXY - sumX * sumY) / (count * sumXX - sumX * sumX);
        double b = (sumY - m * sumX) / count;

        context.write(new Text("Slope (m)"), new DoubleWritable(m));
        context.write(new Text("Intercept (b)"), new DoubleWritable(b));
    }
}
