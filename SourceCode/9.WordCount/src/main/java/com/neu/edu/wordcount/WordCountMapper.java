/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class WordCountMapper extends Mapper<Object,Text,Text,WordFrequency>{
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
	InterruptedException {
        
        String values[] = value.toString().split("\t");
            
            String product = values[3].toString();
            String review = values[13].toString();
            
            Text prod = new Text();
            prod.set(product);
            
            for(String str : review.split(" ")) {
                
                WordFrequency wf = new WordFrequency(1, str);
                context.write(prod,wf);
            }
    }
}