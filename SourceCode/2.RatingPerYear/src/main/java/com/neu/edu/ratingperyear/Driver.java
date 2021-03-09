/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author kaushikpatil
 */
public class Driver {
    public static void main(String[] args) throws Exception{
		
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
		
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(YearMapper.class);
		
        job.setPartitionerClass(YearPartitioner.class);
        job.setGroupingComparatorClass(YearComparator.class);
        job.setSortComparatorClass(SecSortingComparator.class);
		
        job.setMapperClass(YearMapper.class);
        job.setReducerClass(YearReducer.class);
        job.setNumReduceTasks(1);
		
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(AverageCountWritable.class);
		
        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(AverageCountWritable.class);
		
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, outputDir);
		
        FileSystem hdfs = FileSystem.get(conf);
		
        if (hdfs.exists(outputDir)){
            hdfs.delete(outputDir, true);
        }
		
        int code = job.waitForCompletion(true) ? 0 : 1;
            System.exit(code);
    }
}