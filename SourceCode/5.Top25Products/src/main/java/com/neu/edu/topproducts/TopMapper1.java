/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.topproducts;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class TopMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable> {
    
    private Text text = new Text();
    private FloatWritable score = new FloatWritable();
    
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        if(key.get()==0){
            return;
        }
        
        else{
            
            String[] line = value.toString().split("\\t");
            String productid = line[3].trim();
            float rating = Float.valueOf(line[7].trim());
            text.set(productid);
            score.set(rating);
            context.write(text,score);
        }
    }
}