package me.xiaoqian.rabbitmq;

import static org.junit.Assert.*;

import org.junit.Test;

public class RabbitMqUtilTest {
	
	String QUEUE_NAME = "TEST.ONE2ONE";
	String ONE_TO_MORE_QUEUE_NAME = "TEST.ONE2MORE";
	 
	@Test
	public void testOne2OneProducer() {
		boolean result = RabbitMqUtils.one2OneProducer(QUEUE_NAME,"dlete from airline where id=3");
		assertTrue(result);
	}
	
	@Test
	public void one2OneConsumer() {
		RabbitMqUtils.one2OneConsumer(QUEUE_NAME);
		assertTrue(true);
	}
	@Test
	public void testOne2MoreProducer() {
		for(int i=0;i<100;i++) {
			boolean result = RabbitMqUtils.one2MoreProducer(ONE_TO_MORE_QUEUE_NAME,"dlete from airline where id=3");
			assertTrue(result);
		}
	}
	
	@Test
	public void one2MoreConsumer() {
		RabbitMqUtils.one2MoreConsumer(ONE_TO_MORE_QUEUE_NAME);
		assertTrue(true);
	}
	
	
	
	

}
