package com.github.mralfaris.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookFulfillmentApp {

	static Logger logger = LoggerFactory.getLogger(BookFulfillmentApp.class);	
	private static String bootstrapServer = "localhost:9092"; //kafka
	private static final String NEW = "New";
	private static final String SHIPPED = "Shipped";
	private static final String DELIVERED = "Delivered";
	private static String orderTopic = "order";
	private static String statusTopic = "order_status";
	private static List<String> orders = new ArrayList<String>();
	private static List<Integer> deliveredOrders = new ArrayList<Integer>();
	
	public static void main(String[] args) throws InterruptedException {
		
		while (true) {
			checkNewOrders();
			
			for (String orderNumber : orders) {
				changeOrderStatus(SHIPPED, orderNumber);			
			}
			
			Thread.sleep(6000);
			
			for (String orderNumber : orders) {
				changeOrderStatus(DELIVERED, orderNumber);
			}
			
			for (int orderIndex : deliveredOrders) {
				orders.remove(orderIndex);
			}
		}
	}

	
	private static void checkNewOrders() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "order_status");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		//consumer.subscribe(Arrays.asList(orderTopic));
		TopicPartition tp = new TopicPartition(orderTopic, 0);
		consumer.assign(Arrays.asList(tp));
		consumer.seekToEnd(Arrays.asList(tp));
		
		
		logger.info("Waiting for orders....");
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
		
		for (ConsumerRecord<String, String> record : records) {
			logger.info("New order received");
			
			String value = record.value();
			
			StringTokenizer st = new StringTokenizer(value, ",");
			
			String[] orderValues = new String[4];
			
			int i = 0;
			while (st.hasMoreTokens()) {
				orderValues[i] = st.nextToken();
				i++;
			}
			
			logger.info("Order Type = " + orderValues[0]);
			logger.info("Order Number = " + orderValues[1]);
			logger.info("Order Name = " + orderValues[2]);
			logger.info("Order Quantity = " + orderValues[3]);
			
			orders.add(orderValues[1]);
			
			changeOrderStatus(NEW, orderValues[1]);
		}
		
	}
	
	private static void changeOrderStatus(String orderStatusValue, String orderValue) {
		
		logger.info("orderNumber = " + orderValue + ", orderStatus = " + orderStatusValue);
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);		
		ProducerRecord<String, String> producerRecord = 
				new ProducerRecord<String, String>(statusTopic, orderValue + "," + orderStatusValue);
		
		try {
			producer.send(producerRecord).get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (DELIVERED.equals(orderStatusValue)) {
			deliveredOrders.add(orders.indexOf(orderValue));
		}
	}
}
