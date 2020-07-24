# Postgres Replication utility documentation 

## Purpose

Read data from WAL(Write ahead log) file i.e. in binary format and convert that WAL data to readable JSON format using Wal2JSON library and store that JSON data in database/S3/Cloud for auditing purpose.

## Overview

For auditing the applications we usually follow various techniques like oracle trigger, hibernate event etc. depending upon what suit to our application. 

In our case after so many discussions we are agree upon to implement auditing system using WAL file. In postgres database there are two types of replication physical and logical. In our case we are using logical replication. Logical replication allows changes from a database to be streamed in real-time to an external system. The difference between physical replication and logical replication is that logical replication sends data over in a logical format whereas physical replication sends data over in a binary format. Additionally, logical replication can send over a single table, or database.Binary replication replicates the entire cluster in an all or nothing fashion which is to say there is no way to get a specific table or database using binary replication.

Prior to logical replication keeping an external system synchronized in real time was problematic. The application would have to update/invalidate the appropriate cache entries, re-index the data in your search engine, send it to your analytics system, and so on. This suffers from race conditions and reliability problems. For example, if slightly different data gets written to two different datastores (perhaps due to a bug or a race condition), the contents of the datastores will gradually drift apart — they will become more and more inconsistent over time. Recovering from such gradual data corruption is difficult.

Logical decoding takes the database’s write-ahead log (WAL), and gives us access to row-level change events: every time a row in a table is inserted, updated or deleted, that’s an event. Those events are grouped by transaction and appear in the order in which they were committed to the database. Aborted/rolled-back transactions do not appear in the stream. Thus, if you apply the change events in the same order, you end up with an exact, transactionally consistent copy of the database. It's looks like the Event Sourcing pattern that you previously implemented in your application, but now it's available out of the box from the PostgreSQL database.

For access to real-time changes PostgreSQL provides the streaming replication protocol. Replication protocol can be physical or logical. Physical replication protocol is used for Master/Secondary replication. Logical replication protocol canbe used to stream changes to an external system.

## Software Requirements:

* [Java 1.8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Maven](https://maven.apache.org/download.cgi)
* [postgresql-10](https://www.postgresql.org/download/)
* [kafka](https://kafka.apache.org/quickstart)

## Libraries Used:

* [Wal2JSON](https://github.com/eulerto/wal2json)
``Refer git this documentation of Wal2JSON installation.``

## Configuration for postgres
To configure Postgres replication follow below link,

[Logical Replication Configuration](https://jdbc.postgresql.org/documentation/head/replication.html)

Below properties are refered from above link,

postgresql.conf
* Propertymax_wal_sendersshould be at least equal to the number of replication consumers
* Propertywal_keep_segmentsshould contain count wal segments that can't be removed from database.
* Propertywal_levelfor logical replication should be equal tological.
* Propertymax_replication_slotsshould be greater than zero for logical replication, because logical replication can't work without replication slot.
``
Note: All this properties are commented by default. Need to uncomment.
``
``` Example:
max_wal_senders=4 -> max number of walsender processes
wal_keep_segments=4 -> in logfile segments, 16MB each; 0 disables
wal_level=logical -> minimal, replica or logical
max_replication_slots=4 -> max number of replication slots
```

## Build the app using maven

`mvn package`

## Run executable jar using java -jar

`java -jar target/psql-0.0.1-SNAPSHOT.jar`

## To run the app without packaging it using maven command line

`mvn spring-boot:run`
