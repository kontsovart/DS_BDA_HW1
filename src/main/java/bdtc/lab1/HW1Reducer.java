package bdtc.lab1;

import bdtc.lab1.tools.metricIdWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class
 * Input key {@link metricIdWritable}
 * Input value {@link FloatWritable}
 * Output key {@link metricIdWritable}
 * Output value {@link FloatWritable}
 */
public class HW1Reducer extends Reducer<metricIdWritable, FloatWritable, metricIdWritable, FloatWritable> {



    /**
     * override map map function to average metrics
     * @param metric key
     * @param values iterable of values
     * @param context reducer context
     * @throws IOException in context.write()
     * @throws InterruptedException in context.write()
     */
    @Override
    protected void reduce(metricIdWritable metric, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0F;
        float count = 0.0F;
        while (values.iterator().hasNext()) {
            sum += values.iterator().next().get();
            count += 1;
        }
        context.write(new metricIdWritable(metric.getMetricName(), metric.getMetricTimestamp(), metric.getScaleText()),
                new FloatWritable(sum/count));
    }
}
