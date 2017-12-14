package com.hai.avro.mr;

/**
 * Created by as on 2017/3/26.
 */
public class NcdcRecordParser {

    private boolean validTemperature;
    private Integer yearInt;
    private Object airTemperature;
    private Object stationId;

    public void setValidTemperature(boolean validTemperature) {
        this.validTemperature = validTemperature;
    }

    public void setYearInt(Integer yearInt) {
        this.yearInt = yearInt;
    }

    public void setAirTemperature(Object airTemperature) {
        this.airTemperature = airTemperature;
    }

    public void setStationId(Object stationId) {
        this.stationId = stationId;
    }

    public void parse(String s) {
    }

    public boolean isValidTemperature() {
        return validTemperature;
    }

    public Integer getYearInt() {
        return yearInt;
    }

    public Object getAirTemperature() {
        return airTemperature;
    }

    public Object getStationId() {
        return stationId;
    }
}
