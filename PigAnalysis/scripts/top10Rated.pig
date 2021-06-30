infiles = load '/home/sagarshah95/Desktop/BigDataProject_pigAnalysis/data/youtubeDataset.csv' using PigStorage(',') as 
(videoid:chararray,uploader:chararray,age:int,category:chararray,length:int,views:int,rate:int,rating:int,comments:int,related_id:chararray);
files = FILTER infiles BY category is not null;
order_rated_video = order files by rating desc;
top10_rated_video = limit order_rated_video 10;
final_top10_rated_video = foreach top10_rated_video generate $0,$3,$7;
STORE final_top10_rated_video INTO 'Top10Rated.txt' using PigStorage('|');
