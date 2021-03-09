/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingreviewjoin;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class RatingReviewMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text k = new Text();
    private Text v = new Text();

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            return; 
        }

        String[] Input = value.toString().split("\\t");
        String productid = Input[0].trim();
        if (productid == null) {
            return;
        }
        
        k.set(productid);
        v.set("*" + value.toString());
        context.write(k, v);
    }
}