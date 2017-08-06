package me.zhaomeng.spark;

import me.zhaomeng.spark.SparkEngine;

import org.junit.Test;

public class SparkEngineTest{

	@Test
	public void testAdviseEmailByUser(){
		SparkEngine re = new SparkEngine();
		int num = 10;
		String user = "kf@midai.com";
		String email = re.adviseEmailByUser(user, num);
		System.out.println(email);
	}
	
}