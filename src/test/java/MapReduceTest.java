import bdtc.lab1.tools.metricIdWritable;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, metricIdWritable, FloatWritable> mapDriver;
    private ReduceDriver<metricIdWritable, FloatWritable, metricIdWritable, FloatWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, metricIdWritable, FloatWritable, metricIdWritable, FloatWritable> mapReduceDriver;

    private final String testValidMetric = "1,1510670916247,10.0";

    @Before
    public void setup(){
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/files/metric").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("metricScale", "60000");

        HW1Reducer reducer= new HW1Reducer();
        reduceDriver = new ReduceDriver<>(reducer);
        reduceDriver.addCacheFile(new File("src/test/files/metric").getAbsolutePath());
        conf = reduceDriver.getConfiguration();
        conf.set("metricScale", "60000");

        mapReduceDriver = new MapReduceDriver<>(mapper, reducer);
        mapReduceDriver.addCacheFile(new File("src/test/files/metric").getAbsolutePath());
        conf = mapReduceDriver.getConfiguration();
        conf.set("metricScale", "60000");
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidMetric))
                .withOutput(new metricIdWritable("device_1", 1510670880000L, "1m"), new FloatWritable(((float) 10.0)))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<FloatWritable> iterable = new ArrayList<>();
        iterable.add(new FloatWritable(10.0F));
        iterable.add(new FloatWritable(50.0F));
        reduceDriver
                .withInput(new metricIdWritable("device_1", 1510670880000L, "1m"), iterable)
                .withOutput(new metricIdWritable("device_1", 1510670880000L, "1m"),
                        new FloatWritable(30.0F))
                .runTest();
    }

    @Test
    public void testMapperAndReducer() throws IOException {
        String validRow2 = "1,1510670916249,50.0";
        String malformedRow = "aaaa  jnkj lnn";
        String malformedDevice = "198,1510670916247,14.56";
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testValidMetric))
                .withInput(new LongWritable(), new Text(malformedRow))
                .withInput(new LongWritable(), new Text(validRow2))
                .withInput(new LongWritable(), new Text(malformedDevice))
                .withOutput(new metricIdWritable("device_1", 1510670880000L, "1m"),
                        new FloatWritable(30.0F))
                .runTest();
    }
}
