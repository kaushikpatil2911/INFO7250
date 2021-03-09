/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.overallrating;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author kaushikpatil
 */
public class AveragePartitioner extends Partitioner <ProductsWritable, NullWritable>{

    @Override
    public int getPartition(ProductsWritable arg0, NullWritable arg1, int numOfReducerTasks) {
        int hash = arg0.hashCode();
        int partition = hash % numOfReducerTasks;
        return partition;
    }
}