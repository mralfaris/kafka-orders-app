FROM centos:latest

MAINTAINER Moayyad Alfaris <moayyad.alfaris@gmail.com>

RUN yum -y install java-1.8.0-openjdk
RUN java -version

RUN mkdir /opt/kafka

WORKDIR /opt/kafka

RUN yum search wget
RUN yum -y install wget

RUN wget https://archive.apache.org/dist/kafka/2.0.0/kafka_2.12-2.0.0.tgz
RUN tar -xvf kafka_2.12-2.0.0.tgz

ADD start-zookeeper.sh /start-zookeeper.sh

RUN chmod +x /start-zookeeper.sh

WORKDIR /opt/kafka/kafka_2.12-2.0.0/bin

EXPOSE 2181

CMD [ "/start-zookeeper.sh" ] 