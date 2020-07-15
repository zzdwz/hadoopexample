package com.datasoft.groupsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(int orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (this.orderId > o.getOrderId()){
            result = 1;
        }else if (this.orderId < o.getOrderId()){
            result = -1;
        }else{
            if (this.price > o.getPrice()){
                result = -1;
            }else if (this.price < o.getPrice()){
                result = 1;
            }else{
                result = 0;
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price ;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
