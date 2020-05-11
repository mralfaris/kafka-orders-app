package com.github.mralfaris.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderMgmtApp {

	static Logger logger = LoggerFactory.getLogger(OrderMgmtApp.class);
	static boolean createMode = false;
	static boolean statusMode = false;
	static String filePath;
	static String bootstrapServer = "localhost:9092"; //kafka
	static String orderTopic = "order";
	static String statusTopic = "order_status";
	static String bookKey = "book";
	static String computerKey = "computer";
	
	
	public static void main(String[] args) {
		
		parseArgs(args);
		
		/*
		if (createMode && !statusMode && !"".equals(filePath)) {
			logger.info("inside create option");
			logger.info("file path = " + filePath);
			
			createOrder();
			
		} else if (!createMode && statusMode && filePath == null) {
			logger.info("inside status option");
			logger.info("file path = " + filePath);
			
			checkOrderStatus();
			
		} else {
			logger.info(""
					+ "Usage:"
					+ "	-c - to create an order\n"
					+ " -s - to check on order status"
					+ " -f - to provide input file for the order");
		}
		*/
		
		createOrder();
		receiveOrderStatus();
	}
	
	private static void createOrder() {
		
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		CSVParser csvParser = null;
		try {
						
			BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
			csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
			KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
			
			for (CSVRecord csvRecord : csvParser) {
				
				String orderType = csvRecord.get(0);
				String currentRecordOrderType = "";
				int partitionNumber = 0;
				
				if ("b".equals(orderType)) {
					currentRecordOrderType = bookKey;
					partitionNumber = 0;
				} else if ("c".equals(orderType)) {
					currentRecordOrderType = computerKey;
					partitionNumber = 1;
				}
				
				String record = csvRecord.get(0) + 
								","  + csvRecord.get (1) +
								","  + csvRecord.get (2) +
								","  + csvRecord.get (3);
								
				
				logger.info("orderType = " + currentRecordOrderType);
				
				ProducerRecord<String, String> producerRecord = 
						new ProducerRecord<String, String>(orderTopic, partitionNumber, currentRecordOrderType, record);
				
				try {
					producer.send(producerRecord).get();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (IOException ioException) {
			logger.error("Encountered error reading file", ioException);
		} finally {
			try {
				csvParser.close();
			} catch(IOException ioe) {}
		}
		
	}
	
	private static void receiveOrderStatus() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "status");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(statusTopic));
		
		while (true) {
			logger.info("Waiting for order status....");
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
			
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Order status received");
				
				String value = record.value();
				
				StringTokenizer st = new StringTokenizer(value, ",");
				
				String[] orderValues = new String[2];
				
				int i = 0;
				while (st.hasMoreTokens()) {
					orderValues[i] = st.nextToken();
					i++;
				}
				
				logger.info("Order Number = " + orderValues[0] + ", Order Status = " + orderValues[1]);
			}
		}
	}
	
	private static Options orderMgmtOptions() {
		Options options = new Options();
		
		options.addOption(new Option("c", "create", false, "Create Order"));
		options.addOption(new Option("s", "status", false, "Get Order Status"));
		options.addOption(new Option("f", "file", true, "Order File"));
		
		return options;
	}

	private static void parseArgs(String[] args) {
		try {
			
			CommandLineParser commandLineParser = new DefaultParser();
			CommandLine commandLine = commandLineParser.parse(orderMgmtOptions(), args);
			
			createMode = commandLine.hasOption("c");
			statusMode = commandLine.hasOption("s");
			filePath = commandLine.getOptionValue("f");
			
			
		} catch(ParseException pe) {
			logger.error("Error parsing input params");
		}
		
	}
	
}
