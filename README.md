# Processing Big Data For Analytics Applications 

This repository is a detailed documentation of the most relevant work done in an advanced level NYU Computer Science course called ‘Processing Big Data For Analytics Applications’. This course provided me with hands-on experience in handling, processing, and analyzing large datasets using various advanced technologies.

In addition to hands-on experience, the course provided a comprehensive understanding of the platforms, tools, and architectures essential for the scalable management and processing of Big Data.
## Technologies used and learned throughout the course:
**Apache Hadoop and MapReduce**<br>
The first part of this course focused on learning and utilizing the Hadoop framework as it allows for distributed processing of large data sets across clusters of computers using simple programming models. Additionally, we used MapReduce (in java) for processing large data sets with a parallel, distributed algorithm on a cluster.

**Apache HBase**<br>
We used HBase for a distributed, scalable, big data store, providing real-time read/write access to large datasets. 

**Apache Hive**<br>
We utilized Hive for data warehousing and querying the stored data as it facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Hive was particularly useful to learn as it transforms SQL queries into MapReduce and Spark jobs so it was crucial for bridging the gap between data storage and data analysis.

**Presto**<br>
We also used Presto for running interactive analytic queries against data sources of all sizes and residing in different places. Presto is really useful as it allows us to query data wherever it is (HDFS, Kafka, Amazon S3, etc.) and does not require a Hadoop cluster.

**Apache Pig**<br>
The next technology we learned was Apache Pig, which is a platform for analyzing large data sets that consists of a high-level language for writing and evaluating data analysis programs.

**Apache Spark, Spark SQL and Spark Streaming** <br>
A very big part of this course focused on learning different aspects of Apache Spark technology. We began with learning Core Spark as it provided us with in-memory computing capabilities which allowed for much faster data processing and was very beneficial for general-purpose cluster-computing. Next, we used Spark SQL for processing structured data with SQL queries. Finally we learned about Spark Streaming which enables scalable and high-throughput, fault-tolerant stream processing of live data streams in real time.
Additionally, I chose to use Spark for the [Final Project] (BigDataMovieProject "See the project") in this course.

**Other Big Data Tools**<br>
In addition to the previously mentioned tools we also learned the fundamentals of:
- **Oozie**, a workflow scheduler system to manage Apache Hadoop jobs,
- **Zookeeper**, a centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services,
- **Flume**, a service for efficiently collecting, aggregating, and moving large amounts of log data, 
- and **Kafka**, a distributed streaming platform capable of handling trillions of events a day.

## Contents of this Repository

This repository contains some of the coursework, including assignments and a comprehensive class project, where I applied the previously mentioned technologies to different problems and real-world data scenarios. Each folder within the repository is dedicated to a specific assignment or the project and contains detailed documentation, code, and analysis.
### [Assignment 1](https://github.com/PetraIvanovic8/Processing-Big-Data/tree/6a6fe4bd0fba7efbd4c3f79368c19e2d4edc0759/Assignment%201 "See Assignment 1")
In this introductory assignment the focus was on getting familiar with the HDFS interface, creating a simple MapReduce job using Java and compiling and running it in HDFS.

### [Assignment 2](https://github.com/PetraIvanovic8/Processing-Big-Data/tree/6a6fe4bd0fba7efbd4c3f79368c19e2d4edc0759/Assignment%202 "See Assignment 2")
In the first part of the assignment I wrote Python code for a word counting problem and then in the second part I solved the same problem using MapReduce methodology.

### [Assignment 3](https://github.com/PetraIvanovic8/Processing-Big-Data/tree/6a6fe4bd0fba7efbd4c3f79368c19e2d4edc0759/Assignment%203 "See Assignment 3")
In this assignment we analyzed a large unstructured dataset of New York City neighborhoods to find how many times each neighborhood appeared in our problem. In the first part of the assignment I counted the number of neighborhoods for those records (rows) that were of desired length (were not corrupted in any way) and discarded the rest. In the second part of the assignment I further analyzed the dataset to understand how many lines were corrupted (not of desired / expected lengths of records) and in what way they were corrupted. 

### [Assignment 4](https://github.com/PetraIvanovic8/Processing-Big-Data/tree/6a6fe4bd0fba7efbd4c3f79368c19e2d4edc0759/Assignment%204 "See Assignment 4")
This assignment focused on getting us familiar with Google Dataproc and creating a simple project to see how different aspects of the service are used.

### [Assignment 5](https://github.com/PetraIvanovic8/Processing-Big-Data/tree/6a6fe4bd0fba7efbd4c3f79368c19e2d4edc0759/Assignment%205 "See Assignment 5")
In this assignment I used Hive to create 4 tables from data stored on HDFS, merge them appropriately and do preliminary exploration of the data. Next, I used Presto to further analyze these new tables stored in Hive.

### [BigDataMovieProject](BigDataMovieProject "See the project")
This was a final group project that was done for the duration of the second part of the semester. Our project focused on analyzing different aspects of the film industry given the available data. More specifically I focused on analyzing what aspects drive revenue in the industry. Each member of the team cleaned, prepared, and analyzed 1 or more datasets on their own. After this we merged our datasets and analyzed the joint data. This project was mainly done in Spark Scala and we utilized Tableau to present our findings. 
 More details about the project and how to use our code and replicate our findings can be found in the project’s [README](BigDataMovieProject/README.md) file.
