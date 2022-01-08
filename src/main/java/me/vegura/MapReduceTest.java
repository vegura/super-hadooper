package me.vegura;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MapReduceTest {
    public static class E_EMapper
            extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key,
                        Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter)
                throws IOException {
            String line = value.toString();
            String lastToken = null;
            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            String year = tokenizer.nextToken();

            while (tokenizer.hasMoreTokens()) {
                lastToken = tokenizer.nextToken();
            }

            int avgPrice = Integer.parseInt(lastToken);
            output.collect(new Text(year), new IntWritable(avgPrice));
        }
    }

    public static class E_EReduce
            extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key,
                           Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter)
                throws IOException {
            int maxAvg = 30;
            int val = Integer.MIN_VALUE;

            while (values.hasNext()) {
                if ((val = values.next().get()) > maxAvg) {
                    output.collect(key, new IntWritable(val));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(MapReduceTest.class);

        conf.setJobName("max_eletricityunits");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(E_EMapper.class);
        conf.setCombinerClass(E_EReduce.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new org.apache.hadoop.fs.Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new org.apache.hadoop.fs.Path(args[1]));

        JobClient.runJob(conf);
    }
}
