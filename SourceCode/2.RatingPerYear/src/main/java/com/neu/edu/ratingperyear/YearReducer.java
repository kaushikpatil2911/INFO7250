/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kaushikpatil
 */
public class YearReducer extends Reducer <CompositeKeyWritable, AverageCountWritable, CompositeKeyWritable, AverageCountWritable> {

    private AverageCountWritable res = new AverageCountWritable();
	
    @Override
        public void reduce(CompositeKeyWritable arg0, Iterable<AverageCountWritable> arg1, Reducer<CompositeKeyWritable, 
            AverageCountWritable, CompositeKeyWritable, AverageCountWritable>.Context arg2) throws IOException, InterruptedException {
		
		double sum = 0;
		int count = 0;

                for(AverageCountWritable val: arg1) {
                    sum += val.getCount() * val.getRatingAvg();
                    count += val.getCount();
                }
	
                res.setCount(count);
                res.setRatingAvg(sum/count);
                arg2.write(arg0, res);
        }
}