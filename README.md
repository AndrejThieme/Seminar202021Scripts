# Seminar202021Scripts

## Prerequisitories:
	- PostgreSQL database (User, Port, etc. known)
	- Python 3 or higher (needs to be able to run psycopg2)


## 1. Load data into database:
	- Datapackage is very large + a lot of data in the beginning is useless
	- What worked for me:
		1. insert schema
		2. search in dump for specific tables with "awk" 
		   (e.g. "awk '/INSERT INTO public.blocks_transactions_announcement_peers /{print NR;exit}' ethview.sql > output1.txt")
		3. create split dumps of those tables and insert them separately

## 2. Create analytical tables from inserted tables (see script.sql)
	- block_time_analysis is for inter-block-times
	- tr_blocks_query is for transactional analysis
	- anData is for overall amount of data
	- modify select statements if needed (especially tr_blocks_query has potential)

## 3. Visualize results from tables with python (see pythonScript.py)
	- this is a rudimentary structure of the script, change the queries/dataframes/plots to the ones needed
	- important to note: 
		I could not analyse the entire tr_blocks_query (too many entries)
		use "select * from tr_blocks_query where inclusion_index = 1 and extract(day from timestamp)=1;" to truncate
		or find another solution
	- various plots that can be used: 
		https://seaborn.pydata.org/generated/seaborn.lineplot.html
		https://seaborn.pydata.org/generated/seaborn.kdeplot.html
		https://seaborn.pydata.org/generated/seaborn.displot.html
		https://seaborn.pydata.org/generated/seaborn.barplot.html
© 2021 GitHub, Inc.
