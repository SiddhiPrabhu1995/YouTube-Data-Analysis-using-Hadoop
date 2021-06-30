create table YouTube_data_table (vedioid STRING,uploader STRING, age INT, category STRING, length INT, noofviews INT, no_of_comments INT, IDs INT,) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

set hive.cli.print.header=true;

LOAD DATA LOCAL INPATH '/home/sagarshah95/Desktop/BigDataProject_pigAnalysis/data/youtubedata.csv OVERWRITE INTO TABLE YouTube_data_table;


