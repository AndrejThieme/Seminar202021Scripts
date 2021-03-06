--####################################################################################################################################################################################################
--Block Time analysis, with three parameters (node_inclusion_time, node_inclusion_time_alt, downtime_node, downtime_blockchain)
--
--b1_bn, b2_bn: block_number of block b1 and b2
--b1_pt, b2_pt: processed_time of block b1, b2 (database inclusion timestamp)
--b1_timestamp, b2_timestamp: announcement of b1, b2 in blockchain
--node_inclusion_time: (time between block b1 announcement and block b1 inclusion in DB)
--node_inclusion_time_alt (time between block b2 announcement and block b1 inclusion in DB)
--downtime_blockchain (time between block b1 and block b2 announcement
--downtime_node (time between block b1 and block b1+1 inclusion in DB) (Note: node A and A+1 can be not successors of one another, here the indeces of nodes are ordered by their database-timestamp)
--####################################################################################################################################################################################################
DROP TABLE block_time_analysis;

CREATE TABLE block_time_analysis AS WITH ordered AS (SELECT *, row_number() over (ORDER BY block_number) rn FROM blocks) SELECT b1.block_number b1_bn, b2.block_number b2_bn, b1.processed_time b1_pt, b2.processed_time b2_pt, b1.timestamp b1_ts, b2.timestamp b2_ts FROM ordered b1, ordered b2 WHERE b1.rn+1 = b2.rn;

ALTER TABLE block_time_analysis ADD COLUMN node_inclusion_time integer NULL;

ALTER TABLE block_time_analysis ADD COLUMN node_inclusion_time_alt integer NULL;

ALTER TABLE block_time_analysis ADD COLUMN downtime_blockchain integer NULL;

UPDATE block_time_analysis SET b1_pt = (SELECT b1_pt AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE b1_pt <= '2017-12-14 00:00:00' or b1_pt >= '2019-01-16 11:50:57';

UPDATE block_time_analysis SET b2_pt = (SELECT b2_pt AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE b2_pt <= '2017-12-14 00:00:00' or b2_pt >= '2019-01-16 11:50:57';

UPDATE block_time_analysis SET downtime_blockchain = b2_ts - b1_ts;

UPDATE block_time_analysis SET node_inclusion_time_alt = (SELECT ((date_part('day', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp) * 24 + date_part('hour', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('minute', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('second', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp));

UPDATE block_time_analysis SET node_inclusion_time = (SELECT ((date_part('day', b1_pt - (SELECT to_timestamp(b1_ts) AT TIME ZONE 'UTC')::timestamp) * 24 + date_part('hour', b1_pt - (SELECT to_timestamp(b1_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('minute', b1_pt - (SELECT to_timestamp(b1_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('second', b1_pt - (SELECT to_timestamp(b1_ts) AT TIME ZONE 'UTC')::timestamp));

CREATE TABLE node_downtime AS WITH ordered AS (SELECT *, row_number() over (ORDER BY b2_pt) rn FROM block_time_analysis) SELECT b1.b1_bn, b2.b2_bn, b1.b1_pt, b2.b2_pt FROM ordered b1, ordered b2 WHERE b1.rn+1 = b2.rn;

ALTER TABLE node_downtime ADD COLUMN downtime_node integer NULL;

UPDATE node_downtime SET downtime_node = (SELECT ((date_part('day', b2_pt - b1_pt) * 24 + date_part('hour', b2_pt - b1_pt)) * 60 + date_part('minute', b2_pt - b1_pt)) * 60 + date_part('second', b2_pt - b1_pt));

CREATE TABLE block_time_analysis_2 AS (SELECT a.*, b.downtime_node FROM block_time_analysis AS a join node_downtime AS b ON a.b2_bn = b.b2_bn);

DROP TABLE block_time_analysis;

DROP TABLE node_downtime;

ALTER TABLE block_time_analysis_2 RENAME TO block_time_analysis;


--################################################################################################################
--Table to determine time for transactions to be included in blockchain (node)
--
--hash_b: hash of transaction
--block_number, blockhash_b: info of block where transaction was found
--ts: processed time of transaction in DB
--processed_time: processed time of block
--timestamp: announcement of block
--uncle_b: uncle_block of block
--diff: difference between transaction processed time and block processed time
--inclusion_index: number of occurences of transaction in blockchain (aka. number of inclusions until in blockchain)
--################################################################################################################
DROP TABLE tr_blocks_query;

CREATE TABLE tr_blocks_query AS SELECT btr.hash_b, btr.block_number, btr.blockhash_b, btra.ts, b.processed_time, b.timestamp, bu.uncle_b FROM blocks_transactions AS btr INNER JOIN blocks_transactions_announcement AS btra ON btr.hash_b = btra.hash_b INNER JOIN blocks AS b ON btr.blockhash_b = b.hash_b LEFT JOIN blocks_uncles AS bu ON btr.blockhash_b = bu.uncle_b;

UPDATE tr_blocks_query SET processed_time = (SELECT processed_time AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE processed_time <= '2017-12-14 00:00:00' or processed_time >= '2019-01-16 11:50:57';

UPDATE tr_blocks_query SET ts = (SELECT ts AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE ts <= '2017-12-14 00:00:00' or  ts >= '2019-01-16 11:50:57';

ALTER TABLE tr_blocks_query ADD COLUMN diff integer NULL;

UPDATE tr_blocks_query SET diff = (SELECT ((date_part('day', ts - processed_time) * 24 + date_part('hour', ts - processed_time)) * 60 + date_part('minute', ts - processed_time)) * 60 + date_part('second', ts - processed_time));

CREATE TABLE counts AS SELECT hash_b, COUNT(*) as count from tr_blocks_query GROUP BY hash_b;

ALTER TABLE counts RENAME COLUMN count to inclusion_index;

DROP TABLE tr_blocks_query_new;

CREATE TABLE tr_blocks_query_new as (select * from tr_blocks_query left join counts using(hash_b));

DROP TABLE tr_blocks_query;

ALTER TABLE tr_blocks_query_new RENAME TO tr_blocks_query;

--####################################################################################################################
--TABLE to determine analytical data (how many transactions/month/year/index/...
--
--year, month: extracted from processed time timestamp of transaction
--inclusion_index: number of occurences of transaction in blockchain (aka. number of inclusions until in blockchain)
--####################################################################################################################

CREATE TABLE anData AS (SELECT extract(year FROM ts) AS year, extract(month FROM ts) AS month, inclusion_index, count(*) FROM tr_blocks_query_new GROUP BY inclusion_index, year, month);

CREATE TABLE over5 AS (SELECT year, month, sum(count) FROM andata WHERE inclusion_index > 5 group by year, month);

DELETE FROM anData WHERE inclusion_index > 5;

INSERT INTO anData (year, month, inclusion_index, count) SELECT year, month, 6, sum FROM over5;



