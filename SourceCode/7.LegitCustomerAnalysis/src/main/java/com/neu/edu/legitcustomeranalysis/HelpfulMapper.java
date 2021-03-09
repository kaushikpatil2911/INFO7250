/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.legitcustomeranalysis;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class HelpfulMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    
    private Text text = new Text();
    private FloatWritable score = new FloatWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0) {
            return;
        } else {
            String[] line = value.toString().split("\\t");
            String productid = line[3].trim();
            int num = Integer.parseInt(line[8].trim());
            int deno = Integer.parseInt(line[9].trim());
            float rat;
            
            if (deno != 0) {
                rat = num / deno;
            } else {
                rat = (float) 0.0;
            }
            
            text.set(productid);
            score.set(rat);
            context.write(text, score);
        }
    }
}