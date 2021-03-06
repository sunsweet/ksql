# Frequently Asked Questions

| [Overview](/docs#ksql-documentation) |[Quick Start](/docs/quickstart#quick-start) | [Concepts](/docs/concepts.md#concepts) | [Syntax Reference](/docs/syntax-reference.md#syntax-reference) |[Demo](/ksql-clickstream-demo#clickstream-analysis) | [Examples](/docs/examples.md#examples) | FAQ | [Roadmap](/docs/roadmap.md#roadmap) |
|---|----|-----|----|----|----|----|----|


**What are the benefits of KSQL?**

KSQL allows you to query, read, write, and process data in Apache Kafka in real-time and at scale using intuitive SQL-like syntax. KSQL does not require proficiency with a programming language such as Java or Scala, and you don’t have to install a separate processing cluster technology.

**What are the technical requirements of KSQL?**

KSQL only requires:

1. a Java runtime environment
2. access to an Apache Kafka cluster for reading and writing data in real-time. The cluster can be on-premises or in
   the cloud.

We recommend the use of [Confluent Platform](https://www.confluent.io/product/confluent-platform/) or [Confluent Cloud](https://www.confluent.io/confluent-cloud/) for running Apache Kafka.

**Is KSQL owned by the Apache Software Foundation?**

No, KSQL is owned and maintained by [Confluent Inc.](https://www.confluent.io/) as part of its free [Confluent Open Source](https://www.confluent.io/product/confluent-open-source/) product.

**How does KSQL compare to Apache Kafka’s Streams API?**

KSQL is complementary to the Kafka Streams API, and indeed executes queries through Kafka Streams applications. One of the key benefits of KSQL is that it does not require the user to develop any code in Java or Scala.
This enables users to use a SQL-like interface alone to construct streaming ETL pipelines, as well as responding to a real-time, continuous business requests. For full-fledged stream processing applications Kafka Streams remains a more appropriate choice.
As with many technologies each has its sweet-spot based on technical requirements, mission-criticality, and user skill set.

**Does KSQL support Kafka's exactly-once processing semantics?**

Yes, KSQL supports exactly-once processing, which means it will compute correct results even in the face of failures
such as machine crashes.

**Is KSQL ready for production?**

KSQL is a technical preview at this point in time.  We do not yet recommend its use for production purposes.

**Can I use KSQL with my favorite data format (e.g. JSON, Avro)?**

KSQL currently supports formats:

* DELIMITED (e.g. CSV)
* JSON

_Support for Apache Avro is expected soon._

**Is KSQL fully compliant to ANSI SQL?**

KSQL is a dialect inspired by ANSI SQL. It has some differences because it is geared at processing streaming data. For example, ANSI SQL has no notion of “windowing” for use cases such as performing aggregations on data grouped into 5-minute windows, which is a commonly required functionality in the streaming world.

**How do I shutdown a KSQL environment?**

-  To stop DataGen tasks that were started with the `-daemon` flag
   (cf. [Demo](/ksql-clickstream-demo#clickstream-analysis)).

   ```
   $ jps | grep DataGen
   25379 DataGen
   $ kill 25379
   ```

-  Exit KSQL.

   ```
   ksql> exit
   ```

-  Stop Confluent Platform by shutting down all services including Kafka.

   ```
   $ confluent stop
   ```

-  To remove all data, topics, and streams:

   ```
   $ confluent destroy
   ```
