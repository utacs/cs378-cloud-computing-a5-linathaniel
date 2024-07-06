package CleanData;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataCleaningMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final int MIN_TRIP_TIME = 2 * 60; // 2 minutes in seconds
    private static final int MAX_TRIP_TIME = 60 * 60; // 1 hour in seconds
    private static final double MIN_FARE_AMOUNT = 3.0;
    private static final double MAX_FARE_AMOUNT = 200.0;
    private static final double MIN_TRIP_DISTANCE = 1.0;
    private static final double MAX_TRIP_DISTANCE = 50.0;
    private static final double MIN_TOLLS_AMOUNT = 3.0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length != 17) return;

try {
    String pickupDatetime = fields[2];
    int tripTime = Integer.parseInt(fields[4]);
    double tripDistance = Double.parseDouble(fields[5]);
    double pickupLongitude = Double.parseDouble(fields[6]);
    double pickupLatitude = Double.parseDouble(fields[7]);
    double dropoffLongitude = Double.parseDouble(fields[8]);
    double dropoffLatitude = Double.parseDouble(fields[9]);
    double fareAmount = Double.parseDouble(fields[11]);
    double tollsAmount = Double.parseDouble(fields[15]);

    double totalAmount = Double.parseDouble(fields[16]);

    // Apply filters
    if (tripTime < MIN_TRIP_TIME || tripTime > MAX_TRIP_TIME) return;
    if (fareAmount < MIN_FARE_AMOUNT || fareAmount > MAX_FARE_AMOUNT) return;
    if (tripDistance < MIN_TRIP_DISTANCE || tripDistance > MAX_TRIP_DISTANCE) return;
    if (tollsAmount < MIN_TOLLS_AMOUNT) return;

    if (pickupDatetime.isEmpty() || tripDistance <= 0 ||
            pickupLongitude == 0.0 || pickupLatitude == 0.0 ||
            dropoffLongitude == 0.0 || dropoffLatitude == 0.0 || totalAmount <= 0) {
        return;
    }

    context.write(key, value);
}
catch (Exception e){

}
    }
}
