package com.optum.spark.java.app;

import java.awt.Robot;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MultiThreadClass {

	

      public static void main(String args[]) throws Exception {
	    	MultiThreadClass m = new MultiThreadClass();
	    	m.halt();
	    }
	  public static void halt() throws Exception{
	        Robot hal = new Robot();
	        Random random = new Random();
	        while(true){
	            hal.delay(1000 * 60);
	            int x = random.nextInt() % 640;
	            int y = random.nextInt() % 480;
	            hal.mouseMove(x,y);
	        }
	    }



}


