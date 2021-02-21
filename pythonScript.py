import psycopg2
from psycopg2 import Error
import seaborn as sns
import numpy as np
import pandas as pd

try:
    conn = "dbname='db_ethereum' user='testUser' host='localhost' password='password'"
    dsn = "host={} dbname={} user={} password={}".format("localhost", "db_ethereum", "testUser", "password")
    connection = psycopg2.connect(conn)

    cursor = connection.cursor()
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("Your are connected to - ", record, "\n")
    
    
    cursor.execute('select * from anData;')
    #cursor.execute('select * from block_time_analysis;')
    #cursor.execute('select * from tr_blocks_query where inclusion_index >= 2 or (inclusion_index == 1 and extract(day from processed_time) == 1;')
    
    data = [0]*cursor.rowcount
    row = cursor.fetchone()
    i = 0
    while row is not None:
        data[i] = row
        i += 1
        row = cursor.fetchone()


except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
        
dataFrame = pd.DataFrame(data, columns=['year', 'month', 'inclusion_index', 'count'])
#dataFrame = pd.DataFrame(data, columns=['b1_bn', 'b2_bn', 'b1_pt', 'b2_pt', 'b1_ts', 'b2_ts', 'node_inclusion_time', 'node_inclusion_time_alt', 'downtime_blockchain', 'downtime_node'])
#dataFrame = dataFrame.loc[(dataFrame['node_inclusion_time'] <= 1500) & (dataFrame['node_inclusion_time'] >= 0)]
#dataFrame = pd.DataFrame(data, columns=['hash_b', 'block_number', 'blockhash_b', 'ts', 'processed_time', 'timestamp', 'uncle_b', 'diff'])

ax = sns.barplot(data=dataFrame, y='count', x = 'year', estimator=None)
#ax = sns.kdeplot(data=dataFrame, x='node_inclusion_time', common_norm=False, common_grid=True)
#ax = sns.kdeplot(data=dataFrame, x='diff', log_scale=False, bw_adjust=1, common_norm=False, common_grid=True)
fig = ax.get_figure()
fig.savefig("output1.png")
fig.clf()
