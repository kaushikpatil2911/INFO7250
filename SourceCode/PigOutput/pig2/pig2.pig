-- First, we load the raw data from a test dataset

data1 = load '/user/hadoop/AmazonSports.tsv' using PigStorage('\t') AS (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date);

data = STREAM data1 THROUGH `tail -n +2` AS (marketplace, customer_id, review_id, product_id, product_parent, product_title, product_category, star_rating, helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date);

product = GROUP data by star_rating; 

prod_count = FOREACH product GENERATE group as star_rating, COUNT (data.product_id) as count; 

STORE prod_count INTO 'home/kaush/Downloads/output/Q2/';


explain -brief -dot -out ./ abc
