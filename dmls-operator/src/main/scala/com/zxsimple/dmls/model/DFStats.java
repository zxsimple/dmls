package com.zxsimple.dmls.model;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by zxsimple on 2016/10/9.
 */
public class DFStats implements Serializable{
    private ArrayList<Double> count;
    private ArrayList<Double> mean;
    private ArrayList<Double> stddev;
    private ArrayList<Double> min;
    private ArrayList<Double> max;

    public DFStats() {
        count = new ArrayList<Double>();
        mean = new ArrayList<Double>();
        stddev = new ArrayList<Double>();
        min = new ArrayList<Double>();
        max = new ArrayList<Double>();
    }

    public ArrayList<Double> getCount() {
        return count;
    }

    public void setCount(ArrayList<Double> count) {
        this.count = count;
    }

    public ArrayList<Double> getMean() {
        return mean;
    }

    public void setMean(ArrayList<Double> mean) {
        this.mean = mean;
    }

    public ArrayList<Double> getStddev() {
        return stddev;
    }

    public void setStddev(ArrayList<Double> stddev) {
        this.stddev = stddev;
    }

    public ArrayList<Double> getMin() {
        return min;
    }

    public void setMin(ArrayList<Double> min) {
        this.min = min;
    }

    public ArrayList<Double> getMax() {
        return max;
    }

    public void setMax(ArrayList<Double> max) {
        this.max = max;
    }
}
