/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.topproducts;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author kaushikpatil
 */
public class TopMapper2 extends Mapper<LongWritable, Text, FloatWritable, Text> {
    
    public void map(LongWritable key, Text value, Context context) {
        
        String[] row = value.toString().split("\\t");
        String productid = row[0].trim();
        float prodrating = Float.parseFloat(row[1].trim());
        try {
            Text id = new Text(productid);
            FloatWritable prod = new FloatWritable(prodrating);
            context.write(prod, id); }
            catch (Exception e) {
            }
    }
}