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
public class TopReducer1 extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    
    private FloatWritable result = new FloatWritable();
    
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
        
        float sum = 0;
        int count = 0;
        
        for(FloatWritable val:values){
            sum+=val.get();
            count = count+1;
        }
        
        float average = sum/count;
        result.set(average);
        context.write(key,result);
    }
}