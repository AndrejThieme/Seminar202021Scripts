--################################################################################################################
--TABLE to determine times between concurrent blocks
--################################################################################################################
DROP TABLE block_time_difference_query;

CREATE TABLE block_time_difference_query AS WITH ordered AS (SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp) rn FROM blocks) SELECT b1.block_number b1_bn, b1.processed_time b1_pt, b1.rn b1_rn, b2.timestamp b2_ts, b2.block_number b2_bn, b2.rn b2_rn FROM ordered b1, ordered b2 WHERE b1.rn+1 = b2.rn;

UPDATE block_time_difference_query SET b1_pt = (SELECT b1_pt AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE b1_pt <= '2017-12-14 00:00:00' or b1_pt >= '2019-01-16 11:50:57';

ALTER TABLE block_time_difference_query ADD COLUMN diff integer NULL;

UPDATE block_time_difference_query SET diff = (SELECT ((date_part('day', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp) * 24 + date_part('hour', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('minute', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp)) * 60 + date_part('second', b1_pt - (SELECT to_timestamp(b2_ts) AT TIME ZONE 'UTC')::timestamp));

DELETE FROM block_time_difference_query WHERE b1_bn in (with ordered AS (SELECT *, row_number() over (ORDER BY b1_pt) rn FROM block_time_difference_query) SELECT b1.b1_bn FROM ordered b1, ordered b2 WHERE b1.rn + 1 = b2.rn and b1.diff - b2.diff between -20 and 20);

--##############################################################################################################################
--Block Time analysis, with three parameters (node_inclusion_time, node_inclusion_time_alt, downtime_node, downtime_blockchain)
--##############################################################################################################################
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

--#################################################################################
-- TABLE to determine analytical data (how many transactions/month/year/index/...
--#################################################################################

CREATE TABLE anData AS (SELECT extract(year FROM ts) AS year, extract(month FROM ts) AS month, inclusion_index, count(*) FROM tr_blocks_query_new GROUP BY inclusion_index, year, month);

CREATE TABLE over5 AS (SELECT year, month, sum(count) FROM andata WHERE inclusion_index > 5 group by year, month);

DELETE FROM anData WHERE inclusion_index > 5;

INSERT INTO anData (year, month, inclusion_index, count) SELECT year, month, 6, sum FROM over5;


--################################################################################################################
--Table to determine time for transactions to be included in blockchain (node)
--################################################################################################################
DROP TABLE tr_blocks_query;

CREATE TABLE tr_blocks_query AS SELECT btr.hash_b, btr.block_number, btr.blockhash_b, btra.ts, b.processed_time, b.timestamp, bu.uncle_b FROM blocks_transactions AS btr INNER JOIN blocks_transactions_announcement AS btra ON btr.hash_b = btra.hash_b INNER JOIN blocks AS b ON btr.blockhash_b = b.hash_b LEFT JOIN blocks_uncles AS bu ON btr.blockhash_b = bu.uncle_b;

UPDATE tr_blocks_query SET processed_time = (SELECT processed_time AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE processed_time <= '2017-12-14 00:00:00' or processed_time >= '2019-01-16 11:50:57';

UPDATE tr_blocks_query SET ts = (SELECT ts AT TIME ZONE 'Australia/Sydney' AT TIME ZONE 'UTC') WHERE ts <= '2017-12-14 00:00:00' or  ts >= '2019-01-16 11:50:57';

ALTER TABLE tr_blocks_query ADD COLUMN diff integer NULL;

UPDATE tr_blocks_query SET diff = (SELECT ((date_part('day', ts - processed_time) * 24 + date_part('hour', ts - processed_time)) * 60 + date_part('minute', ts - processed_time)) * 60 + date_part('second', ts - processed_time));

CREATE TABLE counts AS SELECT hash_b, COUNT(*) as count from tr_blocks_query GROUP BY hash_b;

ALTER TABLE counts RENAME COLUMN count to inclusion_index;

CREATE TABLE tr_blocks_query_new as (select * from tr_blocks_query left join counts using(hash_b));

DROP TABLE tr_blocks_query;

ALTER TABLE tr_blocks_query_new RENAME TO tr_blocks_query;

--####################################################################################
--list disctinc dates WHERE node wAS down
-- blocknumber is SELECTred because this is the first block WHERE node wAS uptodate
--#####################################################################################

SELECT DISTINCT date(b1_pt) date1, date(b2_pt) date2, avg(downtime_node) FROM block_time_analysis WHERE b1_bn > 2650165 group by date1, date2 ORDER BY avg(downtime_node) desc;



