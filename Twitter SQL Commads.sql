
CREATE TABLE IF NOT EXISTS users 
(created timestamp,
description string,
favouritesCount int,
followersCount int,
friendsCount int,
location string,
mediaCount int,
username string,
verified string)
COMMENT 'Users Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-----
LOAD DATA INPATH '/user/hdfs/TwitterData/TwitterSpam/news_tweeters.csv' INTO TABLE users;

Create TABLE users_temp as SELECT  description ,favouritesCount , followersCount, friendsCount , username , verified from users;


-------------------------------------

PIG COmmands

pig -useHCatalog

users_pig = load 'gs://dataproc-staging-europe-west2-335659031599-84tycneq/TwitterSpam/news_tweeters.csv' using PigStorage(',') 
      as (created:chararray, description:chararray, favouritesCount:int, followersCount:int, friendsCount:int, location:chararray, mediaCount:int, username:chararray, verified:chararray);

tweets_pig = load 'gs://dataproc-staging-europe-west2-335659031599-84tycneq/TwitterSpam/news_tweets.csv' using PigStorage(',') 
      as (content:chararray, datedAt:chararray, hashtags:chararray, likeCount:int, quoteCount:int, replyCount:int, retweetCount:int, retweetedTweet:chararray, sourceLabel:chararray, username:chararray, country:chararray, country_cd:chararray, quoted_content:chararray);


-----------------------------------

STORE users_pig_clean2 into 'users' USING org.apache.hive.hcatalog.pig.HCatStorer();
STORE tweets_pig into 'tweets' USING org.apache.hive.hcatalog.pig.HCatStorer();

describe formatted users;           fs -ls /user/hive/warehouse/users  
describe formatted tweets;          fs -ls /user/hive/warehouse/tweets




--------------------------------------

Create TABLE IF NOT EXISTS cleaned_users 
(description string,
favouritesCount int,
followersCount int,
friendsCount int,
mediaCount int,
username string,
verified string)
COMMENT 'Users Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-------------------------------------------------------------
Tweets


CREATE TABLE IF NOT EXISTS tweets
(content string,
datedAt timestamp,
hashtags string,
likeCount int,
quoteCount int,
replyCount int,
retweetCount int,
retweetedTweet string,
sourceLabel string,
username string,
country string,
country_cd string,
quoted_content string)
COMMENT 'Tweets Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
--------------------------------

LOAD DATA INPATH '/user/hdfs/TwitterData/TwitterSpam/news_tweets.csv' INTO TABLE tweets;
---------------------------
Create TABLE tweets_temp as SELECT  likeCount ,quoteCount , replyCount, retweetCount , sourceLabel , username from tweets;

------------------------------


INSERT INTO cleaned_users SELECT reflect("java.net.URLDecoder", "decode", description, "UTF-8") , favouritesCount , followersCount ,friendsCount , mediaCount , username , verified
 FROM users;


-----------------------------------

SELECT   *
FROM        users U
INNER JOIN  tweets  T
    ON      T.username = T.username

------------------------------

dataproc-staging-europe-west2-335659031599-84tycneq/TwitterSpam

hadoop distcp gs://dataproc-staging-europe-west2-335659031599-84tycneq/TwitterSpam /user/hdfs/TwitterData

dataproc-staging-europe-west2-335659031599-84tycneq/TwitterSpam/news.json

--------------------------------



2) 
users_pig_clean = FILTER users_pig BY description is not null;
users_pig_clean1 = FILTER users_pig_clean BY username is not null;
users_pig_clean2 = FILTER users_pig_clean1 BY verified is not null;


users_pig_clean2 = foreach users_pig_clean generate REPLACE(users_pig_clean.description, '([^a-zA-Z\\s]+)','');


tweets_pig_clean = foreach tweets_pig generate REPLACE(tweets_pig.description, '[^u0000-u007F]+', '');
----------------------------------------------------

CREATE external TABLE IF not exists bagword (bag string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
 WITH SERDEPROPERTIES ("separatorChar"= ",", "quoteChar" = "\") LOCATION '/home/areeb_ireland/bagword.csv' tblproperties("skip.header.line.count"="1");

Load data local inpath '/home/areeb_ireland/bagword.csv' overwrite into table bagword;

create table wordcount as select word from (select explode (split(LCASE(REGEXP_REPLACE(summary,'[\\p{Punct},\\p{Cntrl}]','')),' ')) as word from db.electronics) words;

create table counter as select word, COUNT(*) AS count FROM (SELECT * FROM wordcount LEFT OUTER JOIN bagword on (wordcount.word = bagword.bag) WHERE bag IS NULL) removestopwords GROUP BY word ORDER BY count DESC, word ASC;

------------------------------------
hadoop distcp gs://dataproc-staging-europe-west2-335659031599-84tycneq/bagword.csv /user/areeb_ireland

-------------------------------------

select * from users where verified = "FALSE" and friendsCount < 10 and followersCount < 50