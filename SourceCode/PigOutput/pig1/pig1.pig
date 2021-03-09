-- First, we load the raw data from a test dataset

data1 = load '/user/hadoop/AmazonSports.tsv' using PigStorage('\t') AS (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date);

data = STREAM data1 THROUGH `tail -n +2` AS (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date);

dailydata = GROUP data by review_date;

daily_reviews = FOREACH dailydata GENERATE group as review_date, COUNT(data.review_id) as count;

orderdata = ORDER daily_reviews BY count DESC;

STORE orderdata INTO 'home/kaush/Downloads/output/Q1/' USING PigStorage(',');


explain -brief -dot -out ./ abc

