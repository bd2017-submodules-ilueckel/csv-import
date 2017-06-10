package de.igorlueckel.bigdata2017.lab3.csvimport;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by igorl on 04.06.2017.
 */
public class CsvMapper extends Configured implements Tool {

    private static byte[] hBaseFamily = "data".getBytes();

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(CsvMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "locations");
        job.setMapperClass(Map.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] columns = line.toString().split(",");
            byte[] rowKey = (columns[0] + "_" + columns[1]).getBytes();
            Put put = new Put(rowKey);
            for(int i = 2; i < columns.length; i++) {
                put.addColumn(hBaseFamily, ("column" + i).getBytes(), columns[i].getBytes());
            }
            context.write(new ImmutableBytesWritable(rowKey), put);
            context.getCounter("csv-import", "lines").increment(1);
        }
    }
}
