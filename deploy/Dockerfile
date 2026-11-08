FROM node:24-alpine AS web
WORKDIR /workspace/web
COPY web/package*.json ./
RUN npm ci --no-fund --no-audit
COPY web/ ./
RUN npm run build

FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace
COPY backend/ backend/
COPY --from=web /workspace/web/dist web/dist
RUN mvn -f backend/pom.xml -B -ntp -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app
RUN groupadd --system sale && useradd --system --gid sale sale
COPY --from=build /workspace/backend/target/flash-sale-service-0.1.0.jar app.jar
USER sale
ENV SERVER_ADDRESS=0.0.0.0
EXPOSE 18080
ENTRYPOINT ["java", "-Dfile.encoding=UTF-8", "-jar", "/app/app.jar"]
