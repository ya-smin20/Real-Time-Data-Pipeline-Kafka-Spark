## Project Overview

This project implements a real-time data pipeline that ingests stock market data using Apache Kafka and processes it using Apache Spark. The pipeline consists of a producer that simulates sending stock price data to a Kafka topic and a consumer that reads this data, processes it, and performs necessary transformations.

The main goal of this project is to demonstrate the capabilities of stream processing and the integration of Kafka and Spark for handling real-time data workflows.


### MongoDB
The structured data is pushed to a MongoDB collection with the following schema:

| Keys             | Data Type    |
|------------------|--------------|
| _id              | ObjectId     |
| stock_symbol     | String       |
| timestamp        | String       |
| open             | Double       |
| high             | Double       |
| low              | Double       |
| close            | Double       |
| volume           | Double       |