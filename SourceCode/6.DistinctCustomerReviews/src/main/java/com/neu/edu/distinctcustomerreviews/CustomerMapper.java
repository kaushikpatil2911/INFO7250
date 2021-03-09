/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.distinctcustomerreviews;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class CustomerMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text product = new Text();
    private Text customerid = new Text();
    
    public void map(LongWritable key, Text values, Context context) throws IOException {
        if (key.get() == 0) {
            return;
        }
        
        String[] tokens = values.toString().split("\\t");
        customerid.set(tokens[1]);
        product.set(tokens[3]);
        
        try {
            context.write(product, customerid);
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
