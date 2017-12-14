package com.hai.avro.source;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by as on 2017/3/26.
 */
public class MyUser implements Writable, Serializable {

    private String name;
    private Integer favoriteNumber;
    private String favoriteColor;

    public MyUser() {
    }

    public MyUser(String name, Integer favoriteNumber, String favoriteColor) {
        this.name = name;
        this.favoriteNumber = favoriteNumber;
        this.favoriteColor = favoriteColor;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getFavoriteNumber() {
        return favoriteNumber;
    }

    public void setFavoriteNumber(Integer favoriteNumber) {
        this.favoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        this.favoriteColor = favoriteColor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.favoriteNumber);
        out.writeUTF(this.favoriteColor);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.favoriteNumber = in.readInt();
        this.favoriteColor = in.readUTF();
    }
}
