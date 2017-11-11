package ru.bmstu.hadoop.lab4;

import java.io.Serializable;

/**
 * Created by madina on 11.11.17.
 */
class SerializedData implements Serializable {
    private int originAirportID;
    private int destAirportID;
    private float delayTime;
    private float isCancelled;
    private float maxDelay;
    private int amountOfDelayedAndCanceled;
    private int amountOfAll;
    private float percentages;

    SerializedData(int originAirportID, int destAirportID, float delayTime, float isCancelled) {
        //this.originAirportID = originAirportID;
        //this.destAirportID = destAirportID;
        this.delayTime = delayTime;
        this.isCancelled = isCancelled;

    }

    SerializedData(/*int originAirportID, int destAirportID, */float delayTime, float isCancelled, float maxDelay,int amountOfDelayedAndCanceled, int amountOfAll){
        //this.originAirportID = originAirportID;
        //this.destAirportID = destAirportID;
        this.delayTime = delayTime;
        this.isCancelled = isCancelled;
        this.amountOfDelayedAndCanceled=amountOfDelayedAndCanceled;
        this.maxDelay=maxDelay;
        this.amountOfAll=amountOfAll;

    }

    SerializedData(float maxDelay,int amountOfDelayedAndCanceled, int amountOfAll){
        this.amountOfDelayedAndCanceled=amountOfDelayedAndCanceled;
        this.maxDelay=maxDelay;
        this.amountOfAll=amountOfAll;
        this.percentages=100*amountOfDelayedAndCanceled/amountOfAll;
    }
    public float getDelayTime() {
        return this.delayTime;
    }

    public float getIsCancelled() {
        return this.isCancelled;
    }

    public float getMaxDelay(){
        return this.maxDelay;
    }

    public int getAmountOfDelayedAndCanceled(){return this.amountOfDelayedAndCanceled;}
    public int getAmountOfAll(){return this.amountOfAll;}

}