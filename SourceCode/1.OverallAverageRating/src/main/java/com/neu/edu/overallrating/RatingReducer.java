/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.overallrating;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author kaushikpatil
 */
public class RatingReducer extends Reducer <ProductsWritable, AverageCountWritable, ProductsWritable, AverageCountWritable> {
	
    private AverageCountWritable res = new AverageCountWritable();

    public void reduce(ProductsWritable key, Iterable <AverageCountWritable> values, Context context) throws IOException, InterruptedException {
		
        float sum = 0;
        int count = 0;

            for(AverageCountWritable val: values) {
                sum += val.getCount() * val.getRatingAvg();
                count += val.getCount();
            }
	
        res.setCount(count);
        res.setRatingAvg(sum/count);
        context.write(key, res);
    }
}