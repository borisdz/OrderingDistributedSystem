FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app
COPY OrderingDistributedSystem/.mvn/ .mvn
COPY OrderingDistributedSystem/mvnw OrderingDistributedSystem/pom.xml ./
RUN sed -i 's/\r$//g' mvnw && chmod +x mvnw
COPY Distributed-System-Contracts /contracts
RUN ./mvnw -f /contracts/pom.xml install -DskipTests
COPY OrderingDistributedSystem/src ./src
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","app.jar"]