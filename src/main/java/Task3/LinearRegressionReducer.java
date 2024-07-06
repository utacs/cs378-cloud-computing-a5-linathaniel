package Task3;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.ArrayRealVector;

public class LinearRegressionReducer extends Reducer<Text, Text, Text, Text> {
    private static final double LEARNING_RATE = 0.01;
    private static final int NUM_ITERATIONS = 100;
    private double[] weights = {0.1, 0.1, 0.1, 0.1};

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<double[]> data = new ArrayList<>();
        for (Text value : values) {
            String[] features = value.toString().split(",");
            try {
                double x1 = Double.parseDouble(features[0]);
                double x2 = Double.parseDouble(features[1]);
                double x3 = Double.parseDouble(features[2]);
                double x4 = Double.parseDouble(features[3]);
                double y = Double.parseDouble(features[4]);
                data.add(new double[]{x1, x2, x3, x4, y});
            } catch (NumberFormatException e) {
                // Handle exception if needed
            }
        }

        int n = data.size();
        int m = 4;
        double[][] XArray = new double[n][m];
        double[] yArray = new double[n];

        for (int i = 0; i < n; i++) {
            XArray[i] = new double[]{data.get(i)[0], data.get(i)[1], data.get(i)[2], data.get(i)[3]};
            yArray[i] = data.get(i)[4];
        }

        RealMatrix X = new Array2DRowRealMatrix(XArray);
        RealVector y = new ArrayRealVector(yArray);

        RealVector mean = new ArrayRealVector(m);
        RealVector std = new ArrayRealVector(m);

        // Calculate mean
        for (int j = 0; j < m; j++) {
            mean.setEntry(j, X.getColumnVector(j).getL1Norm() / n);
        }

        // Calculate std
        for (int j = 0; j < m; j++) {
            RealVector diff = X.getColumnVector(j).mapSubtract(mean.getEntry(j));
            std.setEntry(j, Math.sqrt(diff.dotProduct(diff) / n));
        }

        // Standardize
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (std.getEntry(j) != 0) {
                    X.setEntry(i, j, (X.getEntry(i, j) - mean.getEntry(j)) / std.getEntry(j));
                } else {
                    X.setEntry(i, j, 0);
                }
            }
        }

        RealVector weightsVector = new ArrayRealVector(weights);

        for (int iteration = 0; iteration < NUM_ITERATIONS; iteration++) {
            RealVector predictions = X.operate(weightsVector);
            RealVector errors = predictions.subtract(y);
            RealVector gradients = X.transpose().operate(errors).mapDivide(n);

            weightsVector = weightsVector.subtract(gradients.mapMultiply(LEARNING_RATE));

            double cost = errors.dotProduct(errors) / (2 * n);
            System.out.println("Iteration " + iteration + ": Cost " + cost + ", Weights " + weightsVector.toString());
        }

        context.write(new Text("weights"), new Text(weightsVector.toString()));
    }
}
