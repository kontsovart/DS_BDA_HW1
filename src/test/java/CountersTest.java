import bdtc.lab1.tools.metricIdWritable;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, metricIdWritable, FloatWritable> mapDriver;

    private final String testMalformedMetric = "mama mila ramu";
    private final String testValidMetric = "1,1510670916247,10.56";


    @Before
    public void setup(){
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = new MapDriver<>(mapper);
        mapDriver.addCacheFile(new File("src/test/files/metric").getAbsolutePath());
        Configuration conf = mapDriver.getConfiguration();
        conf.set("metricScale", "60000");
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testMalformedMetric))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidMetric))
                .withOutput(new metricIdWritable("device_1", 1510670880000L, "1m"),
                        new FloatWritable(((float) 10.56)))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidMetric))
                .withInput(new LongWritable(), new Text(testMalformedMetric))
                .withInput(new LongWritable(), new Text(testMalformedMetric))
                .withOutput(new metricIdWritable("device_1", 1510670880000L, "1m"),
                        new FloatWritable(((float) 10.56)))
                .runTest();

        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }
}
