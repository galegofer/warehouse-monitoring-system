FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml /app/pom.xml
COPY shared/pom.xml /app/shared/pom.xml
COPY shared/src /app/shared/src

COPY central-service/pom.xml /app/central-service/pom.xml
COPY central-service/src /app/central-service/src

COPY warehouse-service/pom.xml /app/warehouse-service/pom.xml
COPY warehouse-service/src /app/warehouse-service/src

RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN useradd -r -u 10001 appuser
USER appuser

COPY --from=build /app/central-service/target/central-service-*.jar /app/central-service.jar

ENTRYPOINT ["java","-jar","/app/central-service.jar"]