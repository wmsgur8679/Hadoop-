package com.C201844100.DepartureDelayCount;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
	
	private int year;
	private int month;
	private int ArriveDelayTime;
	private int departureDelayTime = 0;
	
	private boolean departureDelayAvallable = true;
	
	
	public AirlinePerformanceParser(Text text) {
		try {
			String[] colums = text.toString().split(",");
			
			year = Integer.parseInt(colums[0]);
			
			month = Integer.parseInt(colums[1]);
			
			if(!colums[14].equals("NA")) {
				ArriveDelayTime =  Integer.parseInt(colums[14]);
			}
			
			if(!colums[15].equals("NA")) {
				departureDelayTime = Integer.parseInt(colums[15]);
			} else {
				departureDelayAvallable = false;
			}
		} catch (Exception e) {
			System.out.println("Error parsing a record :" + e.getMessage());
		}
	}
	
	public int getYear() {return year;}
	
	public int getMonth() {return month;}
	
	
	public int getDepartureDelayTime() {return departureDelayTime;}
	
	public int getArriveDelayTime() {return ArriveDelayTime;}
	
	public  boolean isDepartureDelayAvilable() { return departureDelayAvallable;}
	
}
