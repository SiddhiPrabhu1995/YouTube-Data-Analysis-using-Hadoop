infiles = load '/home/sagarshah95/Desktop/BigDataProject_pigAnalysis/data/youtubeDataset.csv' using PigStorage(',') as 
(videoid:chararray,uploader:chararray,age:int,category:chararray,length:int,views:int,rate:int,rating:int,comments:int,related_id:chararray);
files = FILTER infiles BY category is not null;
grpn_for_catagories = group files by category;
top10_viewed_catagories = foreach grpn_for_catagories{
                           sorted = order files by views desc;
                           top10 = limit sorted 10;
                           generate flatten(top10);
};
top10_viewed_by_catagories = foreach top10_viewed_catagories generate $0,$3,$5;
STORE top10_viewed_by_catagories INTO 'Top10ViewedByCatagories.txt' using PigStorage('|');
