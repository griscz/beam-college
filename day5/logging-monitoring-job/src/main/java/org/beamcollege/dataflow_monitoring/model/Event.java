/*Copyright 2021 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose.
 Your use of it is subject to your agreement with Google. */

package org.beamcollege.dataflow_monitoring.model;

import com.sun.org.apache.bcel.internal.generic.D2I;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Event  {

    String sourceTimestamp;
    String factoryCode;
    String machineId;
    String variableName;
    Double dataValue;

    public Event() {}

    public Event(String sourceTimestamp, String factoryCode, String machineId, String variableName, Double dataValue) {
        this.sourceTimestamp = sourceTimestamp;
        this.factoryCode = factoryCode;
        this.machineId = machineId;
        this.variableName = variableName;
        this.dataValue = dataValue;
    }

    public String getSourceTimestamp() {
        return sourceTimestamp;
    }

    public String getFactoryCode() {
        return factoryCode;
    }

    public String getMachineId() {
        return machineId;
    }

    public String getVariableName() {
        return variableName;
    }

    public Double getDataValue() {
        return dataValue;
    }

    @Override
    public String toString() {
        return "Event{" +
                "sourceTimestamp=" + sourceTimestamp +
                ", factoryCode='" + factoryCode + '\'' +
                ", machineIO='" + machineId + '\'' +
                ", variableName='" + variableName + '\'' +
                ", dataValue='" + dataValue + '\'' +
                '}';
    }

    public String toCSV() {
        return String.join(",", sourceTimestamp, factoryCode, machineId, variableName, Double.toString(dataValue));
    }
}