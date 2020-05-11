FROM centos:latest

MAINTAINER Moayyad Alfaris <moayyad.alfaris@gmail.com>

RUN yum -y install java-1.8.0-openjdk
RUN java -version

RUN mkdir /opt/certs

WORKDIR /opt/certs

RUN openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -