/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingreviewjoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author kaushikpatil
 */
public class Driver {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inner Join"); 
        job.setJarByClass(Driver.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingReviewMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingReviewMapper2.class);
        job.getConfiguration().set("join.type", "Inner");
        job.setReducerClass(RatingReviewReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}