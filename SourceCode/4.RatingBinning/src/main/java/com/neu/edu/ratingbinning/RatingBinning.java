/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingbinning;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author kaushikpatil
 */
public class RatingBinning {
    
    public static void main(String[] args)throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Rating Binning");
        
        job.setJarByClass(RatingBinning.class);
        job.setMapperClass(RatingBinningMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
public static class RatingBinningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    
    private MultipleOutputs<Text, NullWritable> mos = null;

    protected void setup(Context context) {
        mos = new MultipleOutputs(context);
    }
    
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    
        if (key.get() == 0) {
        return;
        }
        
    String[] token = value.toString().split("\\t");
    {
        String rating = token[7].trim();
        
        if (rating.equals("1")) { 
            mos.write("bins", value, NullWritable.get(), "Rating_1");
        }
        
        if (rating.equals("2")) { 
            mos.write("bins", value, NullWritable.get(), "Rating_2");
        }
        
        if (rating.equals("3")) {
            mos.write("bins", value, NullWritable.get(), "Rating_3");
        }
        
        if (rating.equals("4")) { 
            mos.write("bins", value, NullWritable.get(), "Rating_4");
        }
        
        if (rating.equals("5")) { 
            mos.write("bins", value, NullWritable.get(), "Rating_5");
        }
    }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException { 
    mos.close();
    }
}
}