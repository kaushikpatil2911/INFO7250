/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.legitcustomeranalysis;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kaushikpatil
 */
public class HelpfulReducer extends Reducer<Text, FloatWritable, FloatWritable, Text> {
    
    private FloatWritable result = new FloatWritable();
    
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        
        float sum = 0;
        int count = 0;
        
        for (FloatWritable val : values) {
            sum += val.get();
            count = count + 1;
        }
        
        float average = (sum / count) * 100;
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        float avg = Float.parseFloat(df.format(average));
        key.set("%" + " " + key);
        result.set(avg);
        context.write(result, key);
    }
}