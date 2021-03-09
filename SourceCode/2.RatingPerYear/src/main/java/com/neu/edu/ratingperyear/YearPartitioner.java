/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author kaushikpatil
 */
public class YearPartitioner extends Partitioner <CompositeKeyWritable, NullWritable>{

    @Override
	public int getPartition(CompositeKeyWritable arg0, NullWritable arg1, int numOfReducerTasks) {
            int hash = arg0.getProductId().hashCode();
            int partition = hash % numOfReducerTasks;
            return partition;
        }
}