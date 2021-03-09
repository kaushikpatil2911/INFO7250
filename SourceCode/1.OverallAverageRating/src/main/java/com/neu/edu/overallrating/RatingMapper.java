/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.overallrating;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class RatingMapper extends Mapper <Object, Text, ProductsWritable, AverageCountWritable> {
    
    private ProductsWritable productsWritable = new ProductsWritable();
    private AverageCountWritable averageCountWritable = new AverageCountWritable();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split("\t");

            if(values[3] != null && values[7] != null && !values[0].equals("marketplace")) {
                String productID = values[3];
                float productRating = Float.parseFloat(values[7]);
			
                productsWritable.setProductsSymbol(productID);
                productsWritable.setratings(productRating);
			
                averageCountWritable.setCount(1);
                averageCountWritable.setRatingAvg(productRating);
                context.write(productsWritable, averageCountWritable);
            }
    }
}