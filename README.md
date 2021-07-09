# YouTube Dataset Analysis - MapReduce Design Patterns using Apache Hadoop, Pig, Hive

## University group project for Advanced Hadoop MapReduce Programming for Big Data Analytics course.

![image](https://user-images.githubusercontent.com/57429405/125012988-10876b00-e039-11eb-8372-eb6d48e8bc24.png)

•	Implemented Apache Hadoop big data framework to analyze data with help of HDFS, map-reduce design patterns, Pig, HBase.

•	Implemented Data flow language Apache pig built on top of Hadoop to execute Pig Scripts for BIg Data Analysis

•	Manifested Datawarehouse using Apache Hive 

### Problem Statement :-

Analyze the Youtube open source API Big dataset using Hadoop, Pig and HIVE based on different column 
fields to provide various comprehensive insights


### Summary :-

The dataset was available on the following Url:

http://netsg.cs.sfu.ca/youtubedata/

We record the following information of a YouTube video in order; they are divided by '\t' in the data file.


video ID   : an 11-digit string, which is unique

uploader   : a string of the video uploader's username

age        : an integer number of days between the date when the video was uploaded and Feb.15,2007 (YouTube's establishment)

category   : a string of the video category chosen by the uploader

length     : an integer number of the video length

views      : an integer number of the views

rate       : a float number of the video rate

ratings    : an integer number of the ratings

comments   : an integer number of the comments

related IDs: up to 20 strings of the related video IDs




### Datasets of User Information:-

We have collected the information about YouTube users. The crawler retrieves information on the
number of uploaded videos and friends of each user from the YouTube API, for a total of more than 1
million users. There is "user.txt", containing the information of number of uploads, watches and friends
in order.




Following MapReduce programs and its different design patterns like mentioned below are 
implemented:
1. Filtering
2. Join Patterns
3. Data Organization
4. Summarization




Following analysis can be performed on the data-set :
1. Calculate Max Rating Total Rating and Total Comment Count by Video ID
2. Moving Rating Average by Video_ID
3. Best Youtuber based on videos uploaded
4. Top 50 Favorite YouTube Videos
5. Total YouTube Videos by Category
6. Implement Binning based on Categories
7. Implement Chaining on Binning result to get Top 25 Videos per category
8. Recommend followers based on connected followers
9. Total Views based on Video ID


Following Pig analysis is performed on the data-set : ( Implemented Visualizations for Pig Analysis found at the bottom in the Pig analysis section )
1. Calculate top 5 Categories of Youtube Videos
2. Calculate top 10 Rated of Youtube Videos
3. Calculate top 10 Rated By Categories of Youtube Videos
4. Calculate top 10 Viewed of Youtube Videos
5. Calculate top 10 Viewed By Categories of Youtube Videos

Following Hive analysis is performed on the data-set :
1. Calculate top 10 channels with maximum number of likes
2. Calculate top 5 categories with maximum number of comments







