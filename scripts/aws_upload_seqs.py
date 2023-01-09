#!/usr/bin/env python3
import optparse
import boto3
import paramiko
import time
import json
import os
from boto3.s3.transfer import TransferConfig
###############
## Functions ##
###############

# function to get arguments
def get_arguments():
    # create parser object
    parser = optparse.OptionParser()

    # add object options
    parser.add_option('-n', '--project_name', dest='project_name', help='Specify Project Name')
    parser.add_option('-d', '--database', dest='db', help='Provide Database to use (16s - ITS)')
    parser.add_option('-s', '--sequences', dest = 'seqs', help = 'Provide File With URL Sequences')
    
    # get args
    (options, arguments) = parser.parse_args()

    # secure empty executions
    if not options.project_name:
        parser.error("[-] Please Specify a Project Name, use --help for more info")
    elif not options.db:
        parser.error("[-] Please Specify Database to use, use --help for more info")
    elif not options.seqs:
        parser.error("[-] Please Introduce path to FILE with Sequences, use --help for more info")

    #return objects
    return options

# function to create AWS EC2 client
def client_aws(ak, sk, rg, service):
    try:
        #declare objects
        clt = boto3.client(service,
                        rg,
                        aws_access_key_id=ak,
                        aws_secret_access_key=sk)
        print("[+] Boto3 client created")

    except Exception as e:
        print("[-] Boto3 client can not be created")
        print(e)

    # return objects
    return clt

# function to create aws s3 client
def resource_aws(ak, sk, rg, service):
    try:
        #declare objects
        res = boto3.resource(service,
                        rg,
                        aws_access_key_id=ak,
                        aws_secret_access_key=sk)
        print("[+] Boto3 resource created")

    except Exception as e:
        print("[-] Boto3 resource can not be created")
        print(e)

    # return objects
    return res

# Load config
def load_config():
    try:
        # Opening JSON file
        f = open('../configs/aws_conf.json')
        # Load JSON file
        aws_conf = json.load(f)
    except Exception as e:
        print("[-] AWS-Config file not found, (/configs/aws_conf.json)")
    return aws_conf

# function to check if project is already created
def folder_exists(s3, db, pName):
    '''
    Folder should exists. 
    Folder should not be empty.
    '''
    
    path = str(db)+'/'+ str(pName)
    resp = s3.list_objects(Bucket='triggersnextflow', Prefix=path, Delimiter='/',MaxKeys=1)
    return 'CommonPrefixes' in resp

# upload fileseqs.txt to s3
def upload_seqs_s3(file, s3, db, pName):
    #open file and store content
    f = open(file, 'r')
    contents = f.read()
    print("[+] Reading Locally URLs from FileSeqs ")

    # create object
    object = s3.Object('triggersnextflow', str(db)+'/' +str(pName)+'/fileseqs_'+ str(pName) + '.txt')
    
    #execute upload
    result = object.put(Body=contents)
    #get response
    res = result.get('ResponseMetadata')

    #check response
    if res.get('HTTPStatusCode') == 200:
        print('[+] FileSeqs Provided Uploaded Successfully')
        return True
    else:
        print('[-] FileSeqs Provived Can Not Be Uploaded')
        return False

# Launches AWS EC2 Instance with AMI ID
def launch_instance_ami(ec2_resource, ami_id, key_pair_name, sec_group_id,subnet_id, ec2_client, type):
    instance_ids = []
    try:
        # create instance
        instances = ec2_resource.create_instances(
            MinCount = 1,
            MaxCount = 1,
            ImageId=ami_id,
            InstanceType=type,
            KeyName=key_pair_name,
            SecurityGroupIds=[
                sec_group_id,
            ],
            SubnetId=subnet_id,
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': 'ec2-nextflow-instance-run'
                        },
                    ]
                },
            ]
        )
        print("[+] Instance Created")
        # fetch instance ID
        for instance in instances:
            print(" - Instance ID: ", instance.id)
            print("[x] Wait until running")
            instance.wait_until_running()
            print("[+] Instance started")
            response = ec2_client.associate_iam_instance_profile(
                IamInstanceProfile={
                    'Arn': 'arn:aws:iam::451861820529:instance-profile/ecsInstanceRole',
                    'Name': 'ecsInstanceRole'
                },
                InstanceId=instance.id
            )
            print(response)
            # wait until running

    except Exception as e:
        print("[-] Instance can not be created")
        raise e
    return instance.id

# Connect to AWS EC2 Instance
# function to execute pipeline using EC2 instance
def execute_pipe(ec2,ssh_key_file, pName, db, instance_id, bucket):
    
    #find target instances
    target_instances = ec2.describe_instances(
        InstanceIds=[
            str(instance_id)
        ]
    )

    print('[+] Target instances obtained')
    #This function will describe all the instances
    #with their current state 

    ids = [] #stro ips

    for k in target_instances['Reservations']:
        for ins in k['Instances']:
            ids.append(ins['PublicIpAddress'])

    print(' - Public IP obtained:', ids[0])

    try:
        print("[+] Validating pem file...")
        k = paramiko.RSAKey.from_private_key_file(ssh_key_file)

        print("[+] Creating SSH client...")

        c = paramiko.SSHClient()

        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        print("[+] Connecting to SSH client")

        c.connect(hostname=ids[0], username="ec2-user", pkey=k, allow_agent=False, look_for_keys=False)

        print("[+] Connected to SSH client")              
    except Exception as e:
        print("[+] Can Not Connect to SSH client")
        print(e)
        raise(e)
    
    #link = "https://gitlab.com/dvilanova/16s_amazon"

    commands = [
        'python3 seqs_dwn_hash.py '+str(bucket)+' '+str(db)+' '+str(pName)+' > execution_'+str(pName)+'.log',
        'python3 logs_upload.py '+str(bucket)+' '+str(db)+' '+str(pName)

    ]
    try:
        for command in commands:
            print("running command: {}".format(command))
            stdin , stdout, stderr = c.exec_command(command)
            print(stdout.read())
            print(stderr.read())
        c.close()
    except Exception as e:
        print(e)
        raise(e)
    return True

# Terminates AWS EC2 Instance
def terminate_instance(ec2_resource, instanceID):
    try:
        instance = ec2_resource.Instance(instanceID)
        instance.terminate()
        print("[x] Instance Terminated")
    except Exception as e:
        print("Can not terminate instance")
        raise e

def main1():
    print("[*] Remote upload to S3")
    # parse arguments
    opt = get_arguments()

    # load configuration file
    aws_conf     = load_config()
    ssh_key_file = "../keys/nextflow.pem"

    # fetch keys
    access_key = aws_conf['aws']['credentials']['access_key']
    secret_key = aws_conf['aws']['credentials']['secret_key']
    region     = aws_conf['aws']['credentials']['region']
    ami_id_sqs = aws_conf['aws']['ami']['id_sqs']
    bucket_sqs = aws_conf['aws']['s3_bucket_seqs']['name']
    type       = aws_conf['aws']['ec2_type']['seqs']
    key_pair   = aws_conf['aws']['keys']['key_pair_name']
    sg_id      = aws_conf['aws']['segurity_groups']['id']
    subnet_id  = aws_conf['aws']['subnets']['id']

    
    # create aws ec2 client
    aws_ec2_cl = client_aws(access_key, secret_key, region, 'ec2')

    # create aws s3 client
    aws_s3_cl = client_aws(access_key, secret_key, region, 's3')

    # create aws s3 resource
    aws_s3_rs = resource_aws(access_key, secret_key, region, 's3')
    
    # create aws ec2 resource
    aws_ec2_rs = resource_aws(access_key, secret_key, region, 'ec2')

    # check if project name exists
    exists = folder_exists(aws_s3_cl, opt.db, opt.project_name)
    
    if exists == True:
        print("[-] Failed to create new dir, project name ", str(opt.project_name), " already created, please use another name or delete project")
        exit

    # upload sequences file to to s3
    upload_seqs_s3(opt.seqs, aws_s3_rs, opt.db, opt.project_name)

    # launch instance
    instance_id = launch_instance_ami(aws_ec2_rs, ami_id_sqs, key_pair, sg_id, subnet_id, aws_ec2_cl, type)

    # CONNECT to instance
    print("[*] Waiting for port 22")
    time.sleep(25)
    print("[*] .")
    time.sleep(15)
    print("[*] ..")
    time.sleep(10)
    print("[*]...")

    # connect to instance and upload seqs into s3
    execute_pipe(aws_ec2_cl, ssh_key_file, opt.project_name, opt.db, instance_id, bucket_sqs)

    # terminate instance
    terminate_instance(aws_ec2_rs, instance_id)

def main2():
    print("[*] Local Upload to S3")
    # function upload sequences
    opt = get_arguments()
    directory = opt.seqs
    # load configuration file
    aws_conf = load_config()
    # fetch params
    db = opt.db
    pName = opt.project_name
    bucket_name = aws_conf['aws']['s3_bucket_seqs']['name']
    access_key = aws_conf['aws']['credentials']['access_key']
    secret_key = aws_conf['aws']['credentials']['secret_key']
    region     = aws_conf['aws']['credentials']['region']
    # create aws s3
    s3 = client_aws(access_key, secret_key, region, 's3')
    # 
    config = TransferConfig(max_concurrency=2)
    # iterate over files in that directory
    for filename in os.listdir(directory):
        ff = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(ff):
            if ff.endswith('.fastq.gz'):
                with open(ff, 'rb') as f:
                    filepath = str(db)+"/"+str(pName)+"/backups/sample/"+str(filename)
                    s3.upload_fileobj(f, bucket_name, filepath, Config=config)  
                    print("[+] File, ", filename, " uploaded to S3")


####################
## Call functions ##
####################

def main():
    opt = get_arguments()

    isFile = os.path.isfile(opt.seqs)

    if isFile:
        main1()
    else:
        main2()

if __name__ == "__main__":
    main()

    



