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

IAM_ROLE_NAME      = config.get('IAM_ROLE', "IAM_ROLE_NAME")


CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
DB_NAME            = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DB_PORT            = config.get("CLUSTER","DB_PORT")

ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                    )

s3 = boto3.resource('s3',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                   )

iam = boto3.client('iam',aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                     region_name='us-east-1'
                  )

redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                       )

def create_iam_role(IAM_ROLE_NAME):
    '''
    Create a new IAM role
    
    INPUT:
    IAM_ROLE_NAME - specify an IAM role name
    
    OUTPUT:
    roleArn - return the role Arn
    '''
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
    
    return roleArn

def create_cluster(CLUSTER_IDENTIFIER,DB_NAME,DB_USER,DB_PASSWORD,roleArn):
    '''
    Create a Redshift cluster
    
    INPUT:
    CLUSTER_IDENTIFIE - A name of the cluster
    DB_NAME - data warehouse name
    DB_USER - data warehouse user name
    DB_PASSWORD - password of the user
    roleArn - a role ARN created by the IAM role
    
    OUTPUT:
    None
    '''
    try:
        response = redshift.create_cluster(
            #HW
            
            ClusterType='multi-node',
            NumberOfNodes=4,
            NodeType='dc2.large',
            #PubliclyAccessible=True,
            
            #Identifiers & Credentials
            DBName=DB_NAME,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            #Port=config.getint('CLUSTER', 'DB_PORT'),
            
            #Roles (for s3 access)
            IamRoles=[roleArn],
            #VpcSecurityGroupIds=[cluster_sg_id]
        )
    except ClientError as e:
        print(f'ERROR: {e}')
        
    while True:
        response1 = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
        cluster_info = response1['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("The cluster is available now!")
            break
        time.sleep(10)
        
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)
        

    endpoint = myClusterProps['Endpoint']['Address']
    print("cluster endpoint: {}".format(endpoint))
        
        
def main():
    
    roleArn = create_iam_role(IAM_ROLE_NAME)
    create_cluster(CLUSTER_IDENTIFIER,DB_NAME,DB_USER,DB_PASSWORD,roleArn)
    
    
    
if __name__ == '__main__':
    main()