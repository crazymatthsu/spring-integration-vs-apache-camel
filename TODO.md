
- use gradle to build the project
- create submodules for different demo 
- goal is to compare spring integration vs apache camel 
- implement sample use cases for both frameworks

- local podman will be used to do integration tests 
- podman compose will spin up kafka 

- for each use base below, create a submodule for spring integration and a submodule for apache camel, 
  and implement the same use case in both frameworks.

- use case 1: 
  - receive FIX 42 messages (35=D) from Kafka
  - async batch multiple messages and batch JDBC insert into sqlite database
  - manual ack back to Kafka after inserting to database 

- use case 2: 
    - receive algo FIX 42 messages (35=D) from Kafka topic algo
    - receive dma FIX 42 messages (35=D) from Kafka topic dma
    - both messages will be in-memory published asynchronously and processed by another thread 
      using the same handler class
    - after processed by the handler class, ack back to Kafka topic algo and dma respectively
