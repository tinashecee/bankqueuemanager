FROM node:10.19.0
WORKDIR /Kafka2
COPY  / ./
RUN npm install 
EXPOSE 3000
CMD ["npm","run","dev"]