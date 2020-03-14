import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
import time

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_ACCESS_KEY_ID      = config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY  = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
IAM_ROLE_NAME      = config.get('IAM_ROLE', "IAM_ROLE_NAME")

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                       )
iam = boto3.client('iam',aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     region_name='us-east-1'
                  )

def delete_cluster(CLUSTER_IDENTIFIER):
    '''
    Delete a Redshift cluster
    
    INPUT:
    CLUSTER_IDENTIFIER - the cluster name that you want to delete
    
    OUTPUT:
    None
    
    '''
    redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("Deleting the cluster %s..." % CLUSTER_IDENTIFIER)
    while True:
        try:
            
            #myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            response1 = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
            cluster_info = response1['Clusters'][0]
            time.sleep(10)
        except:

            print("Deleted!")
            break
        
def delete_iam_role(IAM_ROLE_NAME):
    '''
    Delete an IAM role
    
    INPUT:
    IAM_ROLE_NAME - the IAM role name that you want to delete
    
    OUTPUT:
    None
    
    '''
    iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    

def main():
    delete_cluster(CLUSTER_IDENTIFIER)
    
    delete_iam_role(IAM_ROLE_NAME)


if __name__ == "__main__":
    main()