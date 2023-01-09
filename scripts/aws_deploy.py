import optparse
import boto3
import paramiko
import time
import json

# Fetches arguments
def get_arguments():
    # create parser object
    parser = optparse.OptionParser()

    # add object options
    parser.add_option('-n', '--project_name', dest='project_name', help='Specify Project Name')
    parser.add_option('-d', '--database', dest='db', help='Provide Database to use (16s - ITS)')
    parser.add_option('-l', '--link', dest='link', help='Provide Gitlab Link. ( https://gitlab.com/dvilanova/16s_amazon )')
    
    # get args
    (options, arguments) = parser.parse_args()

    # secure empty executions
    if not options.project_name:
        parser.error("[-] Please Specify a Project Name, use --help for more info")
    elif not options.db:
        parser.error("[-] Please Specify Database to use, use --help for more info")
    elif not options.link:
        parser.error("[-] Please Specify Link to Gitlab, use --help for more info")

    #return objects
    return options

# Creates AWS EC2 resource
def resource_ec2(ak,sk,rg):
    #declare objects
    ec2 = boto3.resource('ec2',
                    rg,
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk)
    # return objects
    return ec2

# Creates AWS EC2 client
def client_ec2(ak, sk,rg):
    #declare objects
    ec2 = boto3.client('ec2',
                    rg,
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk)
    # return objects
    return ec2

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

    except Exception as e:
        print("[-] Instance can not be created")
        raise e
    return instance.id

# Connect to AWS EC2 Instance
# function to execute pipeline using EC2 instance
def execute_pipe(ec2,ssh_key_file, pName, link , db, instance_id):
    
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

    commands = [
        'docker run --pull=always --memory 70g -v /var/run/docker.sock:/var/run/docker.sock -v /nextflow_workdir/'+str(pName)+':/nextflow_workdir/'+str(pName)+' danova/nextflow-aws:22.04.3 '+str(link)+' -profile batch -r main --input_path "s3://triggersnextflow/'+str(db)+'/'+ str(pName) +'/backups/sample/*.fastq.gz" --pairs_path "s3://triggersnextflow/'+str(db)+'/'+ str(pName) +'/backups/sample/*_{R1,R2}.fastq.gz" --outdir_multiqc "s3://triggersnextflow/16s/'+ str(pName) +'/results/" --outdir_finalres "s3://triggersnextflow/16s/'+ str(pName) +'/results/" -w "/nextflow_workdir/'+str(pName) +'"',
        #'sudo rm -rf /nextflow_workdir/*'    
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

def main():
    # get arguments
    opt = get_arguments()

    # load aws-config file
    aws_conf = load_config()

    access_key = aws_conf['aws']['credentials']['access_key']
    secret_key = aws_conf['aws']['credentials']['secret_key']
    region     = aws_conf['aws']['credentials']['region']
    ami_ID     = aws_conf['aws']['ami']['id_nxfl']
    type       = aws_conf['aws']['ec2_type']['pipe']
    key_pair   = aws_conf['aws']['keys']['key_pair_name']
    sg_id      = aws_conf['aws']['segurity_groups']['id']
    subnet_id  = aws_conf['aws']['subnets']['id']
    ssh_key_file = "../keys/nextflow.pem"
    
    # create resources & clients
        # ec2
    ec2_res = resource_ec2(access_key, secret_key,region)
    ec2_clt = client_ec2(access_key, secret_key,region)

    # EXECUTE AMI LAUNCH
    instance_id = launch_instance_ami(ec2_res, ami_ID, key_pair, sg_id, subnet_id,ec2_clt, type)

    
    # CONNECT to instance
    print("[*] Waiting for port 22")
    time.sleep(25)
    print("[*] .")
    time.sleep(15)
    print("[*] ..")
    time.sleep(10)
    print("[*]...")
    execute_pipe(ec2_clt, ssh_key_file , opt.project_name, opt.link , opt.db, instance_id)
    
    # TERMINATE Instance
    terminate_instance(ec2_res, instance_id)

####################
## Call functions ##
####################

if __name__ == "__main__":
    main()