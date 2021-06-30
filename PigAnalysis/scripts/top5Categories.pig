infiles = load '/home/sagarshah95/Desktop/BigDataProject_pigAnalysis/data/youtubeDataset.csv' using PigStorage(',') as 
(videoid:chararray,uploader:chararray,age:int,category:chararray,length:int,views:int,rate:int,rating:int,comments:int,related_id:chararray);
files = FILTER infiles BY category is not null;
grpn_for_catagories = group files by category;
cnt_for_catagories = foreach grpn_for_catagories generate group, COUNT(files.videoid) as counting;
sorted_for_catagories_desc = order cnt_for_catagories by counting desc;
top5_for_catagories = limit sorted_for_catagories_desc 5;
STORE top5_for_catagories INTO  'Top5Catagories.txt' using PigStorage('|');
