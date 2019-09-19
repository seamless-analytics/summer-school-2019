# Analyzing Log files with Spark in Apache Zeppelin

## Preparation
In this example, we will analyse a logfile from a web server using Apache Spark in Zeppelin notebook. We want to find the top-10 IP addresses, that generated the most entries in the logfile. 

Every entry of the log file has the following structure:  
`10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET / HTTP/1.1" 403 202`

So, to get the IP of every log entry, you can use `row.split(" - ")[0]`

The file you have to use should be in your HDFS user directory `/user/studentX/data/logfile`

Since you will use Apache Spark to process the data, you have to add the corresponding interpreter by addind a `%spark` at the beginning of each notebook paragrah. This makes the **SparkContext** `sc` and **SQLContext** `sqlContext` variables available, that are directly linked with a Spark session. The code written in the Zeppelin notebook is executed inside this session.

## Exercise: Find the top-10 IP addresses that generated the most log entries

1. Login with your user credentials (*studentX* with password *StudentX*), create a new notebook and add the Spark interpreter at the beginning of the first paragraph:  
`%spark`  
**Important:** We will use Scala as programming language. Here, you don't need to add the class name to variables. For better clarity of the used types, we added the class names to each variable. To be able to use the `RDD` class name you have to add the following import to your paragraph:  
`import org.apache.spark.rdd.RDD`

2. Read the Log file from HDFS user directory into the RDD by using the SparkContext (sc):  
`val readRDD : RDD[String] = sc.textFile("data/logfile")`

3. Get the IP address of every entry (filter the other information out):  
`val ipRDD : RDD[String] = readRDD.map(line => line.split(" - ")(0))`

4. Prepare the count by generating a PairRDD of the form **(<IP-address>, 1)**:  
`val pairRDD : RDD[(String, Int)] = ipRDD.map(ip => (ip, 1))`

5. Aggregate (sum up) the amount of log entries by combining all data, grouped by key (= ip-address):  
`val ipCountRDD : RDD[(String, Int)] = pairRDD.reduceByKey((a, b) => a+ b)`

6. Switch key (ip-address) and value (count) of the PairRDD, to be able to use sorting function in the next step:  
`val countIpRDD : RDD[(Int, String)] = ipCountRDD.map(ipCount => (ipCount._2, ipCount._1))`

7. Sort the data of form **(count, <ip-address>)** by the key (= count):  
`val sortedRDD : RDD[(Int, String)] = countIpRDD.sortByKey(false)`

8. Get the first 10 elements (the top-10 ip addresses with the highest counts)  
**Info:** TAKE is a action and collects the data on the Driver process:  
`val asList : Array[(Int, String)] = sortedRDD.take(10)`

9. Print the results on the Driver:
```
var i = 0
var countAll = 0
var ip = ""
var count = 0

println("============================== RESULTS ==============================")
for(i <- 0 to 9) {
    ip = asList(i)._2
    count = asList(i)._1
    
    println((i+1) + ") The IP " + ip + " generated " + count + " log entries")
    countAll += count
}
println("=====================================================================")
```

10. Print more information about the data

```
val entries : Long = ipRDD.count
var top10relative : Double = (countAll.toDouble / entries.toDouble) * 100.toDouble

top10relative = Math.round(top10relative * 10) / 10.0

println("============================== RESULTS ==============================")
println("All Log entries: " + entries)
println("Count of the entries coming from top 10 IPs: " + countAll)
println("Relative amount of top-10 IPs: " + top10relative + " %")
println("=====================================================================")
```

### The output should look like this

```
============================== RESULTS ==============================
1) The IP 10.216.113.172 generated 158614 log entries
2) The IP 10.220.112.1 generated 51942 log entries
3) The IP 10.173.141.213 generated 47503 log entries
4) The IP 10.240.144.183 generated 43592 log entries
5) The IP 10.41.69.177 generated 37554 log entries
6) The IP 10.169.128.121 generated 22516 log entries
7) The IP 10.211.47.159 generated 20866 log entries
8) The IP 10.96.173.111 generated 19667 log entries
9) The IP 10.203.77.198 generated 18878 log entries
10) The IP 10.31.77.18 generated 18721 log entries
=====================================================================
All Log entries: 4477843
Count of the entries coming from top 10 IPs: 439853
Relative amount of top-10 IPs: 9.8 %
=====================================================================
```
