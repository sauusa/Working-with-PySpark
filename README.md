# Working-with-PySpark
Using Spark with Python(Spark SQL) to transform the big data. For this exercise, we are using stand alone system.

<b>Note</b>: This exercise is a part of real world example of Retail Channel Management, this was asked in 1 of the Data Engineering inteviews recently. Following are the requirements for this task, and my given solutions accordingly.
-Saurabh Sahu

## Scenario

Imagine that you are working on a data platform solution for a retail channel engagement customer, and your team is looking for the next important metric to deliver to end enterprise (the merchants/retailers). 
The customer provided an insight that the most frequently requested metric by all retailers is an average of how long users spend on their flyers each day. This would allow them to measure the effectiveness of the flyer, as well as having an understanding of what resonates more with their consumers.

Raw Input File: "<b>Data Engineer Take home dataset - dataset.csv</b>"

### Part 1 - Algorithm
Background information for this exercise:
-	Assume that the channel engagement platform is a mobile or tablet application. 
-	Flyers have unique identifiers
-	Merchants have unique identifiers
-	Merchants have multiple flyers

You have been given a sample dataset with the following schema: timestamp, user_id, event, flyer_id, merchant_id
There are a number of events which the mobile app collects as the shoppers the app. These are the  'event" types in the preceding schema.
•	flyer_open - Whenever a flyer is opened by a user, to see the flyer contents 
•	item_open  - Whenever an item is opened by the user, to see the item details 
•	list_flyers  - Whenever a listing of the flyers is shown 
•	shopping_list_open - Whenever a user opens their shopping list 
•	favorite - Whenever a user adds a merchant to their favourites.

Your task is to:
1)	Compute the average time on flyer per user.
2)	Next, generate an output that will back a Business Intelligence (Bl) report that will be shared with our merchant partners.
3)	Explain how your algorithm scales for:
a)	1 Million Events (~10 MB of data)
b)	1 Trillion Events (~10 TB of data)
To write your algorithm, you may use any of: Python, Java.

### Part 1 - Solution 

Task 1) 
I have created a python notebook program to calculate average time spent on flyer per user.
For algorithm, I have used PySpark (Spark SQL dataframe). The following formula is used:

Average time spent per user on flyer = 
                                   (based on Events ‘flyer_open or ‘item_open’, group by user_id + merchant_id)
(last time (max timestamp) when flyer was used – first time (min timestamp) flyer was used)
                                                                 /    number of flyers per user

Please check <b>Retail_Channel_Management.ipynb</b> for details.

The Raw input file is converted into a simple input file for this task:  <b>channel_dataset.csv</b>

Task 2)
The above-mentioned program generates an output report, which includes the following:
User_id, merchant_id, average time spent (in seconds)

The output is a csv format file, which can be easily used by any BI tool.
Please check output file:  <b>result_dataset.csv</b> for details.

Sample result from Microsoft Power BI:
<img src="https://user-images.githubusercontent.com/67065902/100953731-24864f80-34e1-11eb-9026-7c60ca9c1cb5.PNG"/>

Task 3)

I have used Spark for my data pipeline. 
Spark can effectively scale big data, whether its 10 MB of 10 TB file. 
Spark is a distributed data processing framework which uses RDD (Resilient Distributed Datasets) to partition and process the data. 
I am using Spark with Python (PySpark), it has wide variety of functions and techniques which can be used to effectively process the data. One such method is Map Reduce (which is originated from Hadoop)
For, scaling big data file (with >1 Trillion events), I would either partition the dataset into sets of billion events or divide the files based on file size. Usually the input files come in batches, and are placed in a common path. Using spark, we can read the files directly, and process it using Spark.
Spark is in-memory file processing system, and spark sql can effectively optimize the query using Catalyst.

/~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Part 2 - Infrastructure

During your work on Part 1, you discover that the mobile app is not sending all the necessary user behaviour events. You discuss it with the customer and they agree to collect and send the required, new user behaviour events.
Assume that:
•	You are using a distributed pub-sub style technology cluster such as Apache Kafka/AWS Kinesis/Azure EventHubs 
•	Other groups at the customer will need streaming access to the data
Requirements:
•	User behaviour event data needs to be consumable from the pub-sub Cluster within 120 seconds of being created by the mobile application
•	Data Analysis team will need access to an environment to perform on-demand adhoc analysis
•	Data Science team will need cleansed and backfilled data both on stream and batch capacity to build, train and re-train their models

Your task is to: 
•	Design a workflow to move the user behaviour event data from the application to a backend and provide insights into the data pipelines that you foresee
•	Explain how the workflow would provide the data to the batch process in Part 1 Algorithm.
•	Explain any adaptations that your work from Part 1 - Algorithm would need to work as a streaming process.
•	Highlight any important design decisions you make, and describe briefly why you made those decisions. Important parts to address include:
o	Latency
o	Scalability & Data Volume
o	Robustness
o	Failure Modes
o	Delivery Guarantees
o	Technology selections

### Part 2 - Solution

•	Design a workflow to move the user behavior event data from the application to a backend and provide insights into the data pipelines that you foresee

Please see the below diagram to understand the process flow:
 
<img src="https://user-images.githubusercontent.com/67065902/100951409-7ed0e180-34dc-11eb-9c37-62ea9ca44e98.png"/>

Note: I have added following events to make the process more effective
-	Flyer_close (so the time spent by user between flyer_open and flyer_close or item_open can be measured effectively)
-	Checkout (for payment proessing/optional)


•	Explain how the workflow would provide the data to the batch process in Part 1 Algorithm.

Please see the following data flow diagram to understand the process:
<img src="https://user-images.githubusercontent.com/67065902/100951593-da9b6a80-34dc-11eb-842e-86c71c9fad1a.png"/>

#### Steps:
1.	Fetch the event data from user using APIs
2.	Incoming data from web/mobile as input files (csv, json, xml) store in either database or storage.
3.	Collect the relevant data at 1 place, in this case we have used external folder
4.	Using data pipeline (Python), ingest the data into data frames for further processing
5.	Extract, Transform and Load the data, using data framework (PySpark)
6.	Process the output results, store it in hard disk for transmission
7.	Use the output file (csv) to be used by Business Intelligence/Analytics team


•	Explain any adaptations that your work from Part 1 - Algorithm would need to work as a streaming process.

In Part1, I have used Jupyter Notebook, which will be turned into Python Script (.py) file, as data pipeline.
The pipeline can be used by Data Flow Pipeline tools. I would prefer using Apache Airflow (since its developed using Python).
For Streaming process following Algorithm would be used (using Kafka):
Steps:
1.	Start the zookeeper server
2.	Start the Kafka server
3.	Create a topic
4.	Start a Producer (source)
5.	Start a Consumer (Target)
6.	Publish the message (source  target)

•	Highlight any important design decisions you make, and describe briefly why you made those decisions. Important parts to address include:
o	Latency
o	Scalability & Data Volume
o	Robustness
o	Failure Modes
o	Delivery Guarantees
o	Technology selections

##### Latency:
I have used Apache Spark (version 2.3), which brings continuous processing to structured streaming.
This feature gives us low-latency responses on order of around 1ms (instead of the 100ms, we get using other batch processing methods)

##### Scalability & Data Volume:
Spark can handle a huge amount of real-time data. Data volume is not an issue for Big data frameworks. However, the processing depends on the computing capacity of the system.
If we have huge data (ex 500TB) coming in our system, I would better use AWS EMR clusters. 

##### Robustness:
Data quality is an important factor. Since we are using python, the data quality is maintained using internal functions. We have designed our work flow in such a way that we can easily deal with the data loss or corrupt data situation. Moreover, the frameworks which I have used (PySpark, Pandas) doesn’t affect the source data. The output result can also be easily transformed and stored using various functions.

##### Failure Modes:
In case of code failure, we can use Spark Web UI or investigate on-screen error for syntax and other runtime errors.
In case of Data failure, we can use accumulators to trace the cause, and either fix/ignore the record.

##### Delivery guarantees:
Using the data pipelines, we can automate the process, which will guarantee the outcomes.

##### Technology selections:
   Language: Python, because it’s a general-purpose programming language which provides us flexibility
   Data framework: Spark, because its in-memory framework, most effective in terms of handling volume and processing. Also its easy to          use with Python (using PySpark: Spark SQL)
   Pipeline: Apache Airflow/Kafka/ Python scripts
   Data Modeling: Erwin, Visio or Oracle/PG data modeler

### Conclusion:
Spark (PySpark) with python provides flexibility and advantages while working with any types of Big Data. 
We can use other data frames as well, as per the requirements.
This is a sample project, completely built from scratch, so there is a scope for improvements.
If you have any suggestions for improvement or questions, please contact me.
Thank you. 
