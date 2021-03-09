/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author kaushikpatil
 */
public class Driver {
    
    public static void main(String[] args)  throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
            System.exit(-1);
        }
			
	Path inputPath = new Path(args[0]);
	Path outputDir = new Path(args[1]);
        
	Configuration conf = new Configuration(true);

	Job job = Job.getInstance(conf);
	job.setJarByClass(WordCountMapper.class);

	job.setMapperClass(WordCountMapper.class);
	job.setReducerClass(WordCountReducer.class);
	job.setNumReduceTasks(1);
			
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(WordFrequency.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(WordFrequency.class);
			
	FileInputFormat.addInputPath(job, inputPath);
	job.setInputFormatClass(TextInputFormat.class);

	FileOutputFormat.setOutputPath(job, outputDir);

	FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir)) {
            hdfs.delete(outputDir, true);
        }
	
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }
}