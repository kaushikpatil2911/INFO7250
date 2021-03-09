/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.edu.overallrating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author kaushikpatil
 */
public class ProductsWritable implements Writable, WritableComparable<ProductsWritable>{
	
	private String productID;
	private float ratings;
	
	public ProductsWritable() {
            super();
	}

	public ProductsWritable(String ProductsSymbol, float ratings) {
            super();
		
            this.ratings = ratings;
            this.productID = ProductsSymbol;
	}
		
	public String getProductsSymbol() {
            return productID;
	}

	public void setProductsSymbol(String ProductsSymbol) {
            this.productID = ProductsSymbol;
	}

	public float getrating() {
            return ratings;
	}

	public void setratings(float ratings) {
            this.ratings = ratings;
	}
        
        
        
        @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((productID == null) ? 0 : productID.hashCode());
		result = prime * result + Float.floatToIntBits(ratings);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProductsWritable other = (ProductsWritable) obj;
		if (productID == null) {
			if (other.productID != null)
				return false;
		} else if (!productID.equals(other.productID))
			return false;
		if (Float.floatToIntBits(ratings) != Float.floatToIntBits(other.ratings))
			return false;
		return true;
	}
        
        public void readFields(DataInput in) throws IOException {
            ratings = in.readInt();
            productID = in.readUTF();
	}
        
        public void write(DataOutput out) throws IOException {
            out.writeFloat(ratings);
            out.writeUTF(productID);
		
	}
        
        public int compareTo(ProductsWritable o) {
            int result = -1;
                if(ratings >  o.ratings) {
                    result = 1;
                }
                if(ratings == o.ratings) {
                    result = 0;
                }
                if(result == 0) {
			result  = productID.compareTo(o.productID);
		 }
		return result;
        }

	@Override
	public String toString() {
            return "Product = " + productID + "\t";
	}
}