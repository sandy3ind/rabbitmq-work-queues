package com.javagain.amqp;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Worker {

	private static final String TASK_QUEUE_NAME = "task_queue";
	
	public static void main(String[] args) throws Exception {
		
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    final Connection connection = factory.newConnection();
	    final Channel channel = connection.createChannel();
	    
	    // Add durability
	    boolean durable = true;
	    
	    channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
	    
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    /*
	     * This tells RabbitMQ not to give more than one message to a worker at a time. 
	     * Or, in other words, don't dispatch a new message to a worker until it has processed
	     * and acknowledged the previous one. Instead, it will dispatch it to the next worker that is not still busy.
	     */
	    channel.basicQos(1); // accept only one unack-ed message at a time
	    
	    final Consumer consumer = new DefaultConsumer(channel) {
	        @Override
	        public void handleDelivery(String consumerTag, Envelope envelope, 
	        		AMQP.BasicProperties properties, byte[] body) throws IOException {
	          String message = new String(body, "UTF-8");

	          System.out.println(" [x] Received '" + message + "'");
	          try {
	            doWork(message);
	          } finally {
	            System.out.println(" [x] Done");
	            channel.basicAck(envelope.getDeliveryTag(), false);
	          }
	        }
	      };	      
	      boolean autoAck = false;
	      channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
	}
	
	private static void doWork(String task) {
		for (char ch : task.toCharArray()) {
			if (ch == '.') {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException _ignored) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
}
