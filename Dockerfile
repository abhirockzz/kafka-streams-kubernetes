FROM openjdk:8-jre
WORKDIR /
COPY target/kstreams-lower-to-upper-1.0.jar /
CMD ["java", "-jar","kstreams-lower-to-upper-1.0.jar"]