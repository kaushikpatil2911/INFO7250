/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.topproducts;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kaushikpatil
 */
public class TopReducer2 extends Reducer<FloatWritable, Text, FloatWritable, Text> {
    
    int count = 0;
    
    public void reduce(FloatWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        
        for (Text val : value) {
            if (count < 25) {
                context.write(key, val);
            }
            count++;
        }
    }
}