# Build stage
FROM maven:3.8.7-eclipse-temurin-8 AS build
WORKDIR /app
# Force cache bust for pom.xml - Attempt 8
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Run stage
FROM eclipse-temurin:8-jre
WORKDIR /app
COPY --from=build /app/target/flink-streaming-1.0-SNAPSHOT-exec.jar app.jar
COPY --from=build /app/target/flink-streaming-1.0-SNAPSHOT.jar flink-job.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
