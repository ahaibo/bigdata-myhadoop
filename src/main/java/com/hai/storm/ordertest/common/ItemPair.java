package com.hai.storm.ordertest.common;

/**
 * Created by as on 2017/4/2.
 */
public class ItemPair {

    private String item1;
    private String item2;

    public ItemPair() {
    }

    public ItemPair(String item1, String item2) {
        this.item1 = item1;
        this.item2 = item2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ItemPair itemPair = (ItemPair) o;

        if (item1 != null ? !item1.equals(itemPair.item1) : itemPair.item1 != null) return false;
        return item2 != null ? item2.equals(itemPair.item2) : itemPair.item2 == null;
    }

    @Override
    public int hashCode() {
        int result = item1 != null ? item1.hashCode() : 0;
        result = 31 * result + (item2 != null ? item2.hashCode() : 0);
        return result;
    }

    public String getItem1() {
        return item1;
    }

    public void setItem1(String item1) {
        this.item1 = item1;
    }

    public String getItem2() {
        return item2;
    }

    public void setItem2(String item2) {
        this.item2 = item2;
    }
}
