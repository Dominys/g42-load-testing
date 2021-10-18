package com.example.model;

import lombok.Data;

/**
 * @author dominys
 * @since 20.09.21
 */
@Data
public class StatData {
    long lastAccOnTime;
    long utcTime;
    long totalTripMileage;
    long currentTripMileage;
    long totalFuel;
    long currentFuel;
    long vehicleStatus;
}
