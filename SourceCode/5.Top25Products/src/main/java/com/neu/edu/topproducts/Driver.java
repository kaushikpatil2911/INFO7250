/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.topproducts;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author kaushikpatil
 */
public class Driver {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Best 25 Products");
        
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(TopMapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);
        job1.setReducerClass(TopReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        boolean complete = job1.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        
        if (complete) {
            
            Job job2 = Job.getInstance(conf2, "Best 25 Products");
            job2.setJarByClass(Driver.class);
            job2.setMapperClass(TopMapper2.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setSortComparatorClass(TopComparator.class);
            job2.setReducerClass(TopReducer2.class);
            job2.setOutputKeyClass(FloatWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}