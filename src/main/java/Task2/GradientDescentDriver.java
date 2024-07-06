// GradientDescentDriver.java
package Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class GradientDescentDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        double learningRate = 0.001;
        int numIterations = 100;
        double m = 0.0;
        double b = 0.0;

        for (int i = 0; i < numIterations; i++) {
            conf.set("m", String.valueOf(m));
            conf.set("b", String.valueOf(b));
            conf.set("learningRate", String.valueOf(learningRate));

            Job job = Job.getInstance(conf, "Gradient Descent Iteration " + i);
            job.setNumReduceTasks(1);
            job.setJarByClass(GradientDescentDriver.class);
            job.setMapperClass(GradientDescentMapper.class);
            job.setReducerClass(GradientDescentReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + i));

            job.waitForCompletion(true);

            // Read the updated parameters
            Path outputPath = new Path(args[1] + i + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts[0].equals("m")) {
                    m = Double.parseDouble(parts[1]);
                } else if (parts[0].equals("b")) {
                    b = Double.parseDouble(parts[1]);
                }
            }
            br.close();
        }

        System.out.println("Final parameters:");
        System.out.println("m = " + m);
        System.out.println("b = " + b);
    }
}
