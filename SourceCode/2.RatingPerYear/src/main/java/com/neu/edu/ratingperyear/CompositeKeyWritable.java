/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.ratingperyear;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author kaushikpatil
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable>{
	
    private String productId;
    private int year;
	
    public CompositeKeyWritable() {
        super();
    }

    public CompositeKeyWritable(int year, String productId) {
	super();
	this.year = year;
	this.productId = productId;
    }
	
    public String getProductId() {
	return productId;
    }

    public void setProductId(String productId) {
	this.productId = productId;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }
    
    public void readFields(DataInput in) throws IOException {
        productId = in.readUTF();
        year = in.readInt();
    }
    
    public void write(DataOutput out) throws IOException {
        out.writeUTF(productId);
        out.writeInt(year);
    }
    
    public int compareTo(CompositeKeyWritable o) {
		
        int result = -1;
		
        if(year > o.year) {
            result = 1;
        } 
	
        else if (year == o.year) {
            result = 0;
        }
		
        else {
            result = -1;
        }
		
        if(result == 0) {
            result = productId.compareTo(o.productId);
        }
            return result;
    }

    @Override
	public String toString() {
            return "ProductId=" + productId + ",\t"+ "Year=" + year + "\t";
        }	
}