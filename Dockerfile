FROM eclipse-temurin:17-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY conveyor-core/pom.xml conveyor-core/
COPY conveyor-spring-boot-starter/pom.xml conveyor-spring-boot-starter/
COPY conveyor-server/pom.xml conveyor-server/
# poms first, so the dependency layer stays cached until one of them changes
RUN ./mvnw -q dependency:go-offline -pl conveyor-server -am
COPY conveyor-core/src conveyor-core/src
COPY conveyor-spring-boot-starter/src conveyor-spring-boot-starter/src
COPY conveyor-server/src conveyor-server/src
# -am builds the core and starter jars the server depends on
RUN ./mvnw -q -DskipTests package -pl conveyor-server -am && cp conveyor-server/target/conveyor-server-*.jar app.jar

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd --system conveyor
USER conveyor
COPY --from=build /src/app.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
