FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw -q dependency:go-offline
COPY src src
RUN ./mvnw -q -DskipTests package && cp target/conveyor-*.jar app.jar

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd --system conveyor
USER conveyor
COPY --from=build /src/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
