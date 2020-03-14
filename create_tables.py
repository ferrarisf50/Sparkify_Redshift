import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    AWS_ACCESS_KEY_ID      = config.get('AWS','AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY  = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    
    DB_NAME            = config.get("CLUSTER","DB_NAME")
    DB_USER            = config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT               = config.get("CLUSTER","DB_PORT")
    
    redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                       )
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
 

    endpoint = myClusterProps['Endpoint']['Address']
    #roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(endpoint,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()