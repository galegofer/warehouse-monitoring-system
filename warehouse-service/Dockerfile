FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml /app/pom.xml

COPY shared/pom.xml /app/shared/pom.xml
COPY shared/src /app/shared/src

COPY warehouse-service/pom.xml /app/warehouse-service/pom.xml
COPY warehouse-service/src /app/warehouse-service/src

COPY central-service/pom.xml /app/central-service/pom.xml
COPY central-service/src /app/central-service/src

RUN mvn -q -DskipTests -pl warehouse-service -am package

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN useradd -r -u 10001 appuser
USER appuser

COPY --from=build /app/warehouse-service/target/warehouse-service-*.jar /app/warehouse-service.jar

ENV BROKER_URL=tcp://activemq:61616
ENV DESTINATION_NAME=measurements.queue
ENV WAREHOUSE_ID=WH-1
ENV UDP_TEMPERATURE_PORT=3344
ENV UDP_HUMIDITY_PORT=3355

EXPOSE 3344/udp
EXPOSE 3355/udp

ENTRYPOINT ["java","-jar","/app/warehouse-service.jar"]
