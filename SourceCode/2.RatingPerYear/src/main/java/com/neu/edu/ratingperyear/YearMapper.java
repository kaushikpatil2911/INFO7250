/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author kaushikpatil
 */
public class YearMapper extends Mapper <Object, Text, CompositeKeyWritable, AverageCountWritable>{
 
    private AverageCountWritable averageCountRating = new AverageCountWritable();
	
    @Override
        public void map(Object key, Text value, Mapper<Object, Text, CompositeKeyWritable, AverageCountWritable>.Context context) throws IOException, InterruptedException {
		
            String values[] = value.toString().split("\t");
            String productId = null;
                
                int year = 0;
                double rating = 0.0D;
                
                if(values[5] != null && values[7] != null && !values[0].equals("marketplace")) {
                    try {
                        productId = values[3];
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date reviewDate = sdf.parse(values[14]);
                        year = Integer.parseInt(reviewDate.toString().split(" ")[5]);
                        rating = Double.parseDouble(values[7]);
		    	
                        }catch(Exception e) {
                            e.printStackTrace();
                    }

                if(year != 0 && productId != null) {

                    averageCountRating.setCount(1);
                    averageCountRating.setRatingAvg(rating);
                    CompositeKeyWritable ck = new CompositeKeyWritable(year, productId);
				
                    try {
                            context.write(ck, averageCountRating);
                    }catch (Exception e) {
                            e.printStackTrace();
                    }
                }
                }
	}
}