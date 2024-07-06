package Task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinearRegressionMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        try {
            double tripTimeInSecs = Double.parseDouble(fields[4]);
            double tripDistance = Double.parseDouble(fields[5]);
            double fareAmount = Double.parseDouble(fields[11]);
            double tollsAmount = Double.parseDouble(fields[15]);
            double totalAmount = Double.parseDouble(fields[16]);

                context.write(new Text("features"), new Text(tripTimeInSecs + "," + tripDistance + "," + fareAmount + "," + tollsAmount + "," + totalAmount));

        } catch (Exception e) {
        }
    }
}
