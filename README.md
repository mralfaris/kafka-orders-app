# Order Management App

The app is split into
  - Order Management Application: Generates orders from a CSV file provided as input to the Java App.
  - Book Order Fulfillment Application: Receives book orders and processes them.
  - Computer Order Fulfillment Application: Receives computer orders and processes them.

The applications run on Java 8 on Docker Containers. The messaging platform Kafka runs as a Docker Container.

## Design Decisions
  - DD1: There are two topics created of Kafka broker, one for orders and the other one for order status.
  - DD2: Order topic has two partitions, one partition is for Book orders and the second partition is for Computer orders.
  - DD3: Order status is sent for both Book and Computer orders on the same topic.
  - DD4: Order messages are polled by Computer and Book Order Fulfillment Application from the dedicated partition and then processed immediately.
  - DD5: Order status messages are being polled by Order Management Application from order status topic every 10 seconds. Then status is displayed on the screen.
  - DD6: Messages are sent as text (String) with comma-separated values.
  - DD7: TLS to be used to secure the channel of communication between Kafka Broker, Consumer and Producer.
  - DD8: SASL plain will be used for Authentication using Username/Password to connect to Kafka Brokers.
  - DD9: ACL policy will enforce authroization rules on consumers and producers.

## File and Data Format
  - Order input file: is a CSV file having values in the following order (Order Type, Order Id, Item Name, Quantity).
  - Order status message: A comma-separated text with two values (Order Id, Order Status).


## To Do
  - Update Dockerfile configuration Java, Docker and Zookeeper.
  - Complete Docker images push to Dockerhub.
  - Build Docker Compose file.
  - Generate a certificate, update trust store and enable listener with TLS on a new port.
  - Update trust store of Java consumers with the Kafka server certificate.
  - Enable SASL plain for username and password authentication on brokers.
  - Enable ACL for authroization of users on specific topics.
  - Prepare test cases.
  - Clean up Java code.
