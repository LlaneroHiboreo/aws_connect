#!/usr/bin/env python3
import aws_deploy as main_pipe
import aws_upload_seqs as main_upload
import optparse
import boto3
import paramiko
import os
import time
from boto3.s3.transfer import TransferConfig

###############
## Functions ##
###############

# Get arguments
def get_arguments():
    # create parser object
    parser = optparse.OptionParser()

    # add object options
    parser.add_option('-n', '--project_name', dest='project_name', help='Specify Project Name')
    parser.add_option('-d', '--database', dest='db', help='Provide Database to use (16s - ITS)')
    parser.add_option('-s', '--sequences', dest = 'seqs', help = 'Provide File With URL Sequences or path to local directory')
    parser.add_option('-l', '--link', dest='link', help='Provide Gitlab Link. ( https://gitlab.com/dvilanova/16s_amazon )')
    parser.add_option('-c', '--code', dest = 'code', help = 'Code 1: Upload Sequences - Code 2: Upload Sequences + Run Pipeline - Code 3: Run Pipeline')

    # get args
    (options, arguments) = parser.parse_args()

    # secure empty executions
    if not options.project_name:
        parser.error("[-] Please Specify a Project Name, use --help for more info")
    elif not options.db:
        parser.error("[-] Please Specify Database to use, use --help for more info")
    elif not options.seqs:
        parser.error("[-] Please Introduce path to FILE with Sequences or path to DIR with local seqs, use --help for more info")
    elif not options.link:
        parser.error("[-] Please Specify Link to Gitlab, use --help for more info")
    elif not options.code:
        parser.error("[-] Please Specify Code to use, use --help for more info")

    #return objects
    return options

# main function to upload seqs from file
def main_uploads():
    print("[*] Remote upload to S3")
    # parse arguments
    opt = get_arguments()

    # load configuration file
    aws_conf     = main_upload.load_config()
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
    aws_ec2_cl = main_upload.client_aws(access_key, secret_key, region, 'ec2')

    # create aws s3 client
    aws_s3_cl = main_upload.client_aws(access_key, secret_key, region, 's3')

    # create aws s3 resource
    aws_s3_rs = main_upload.resource_aws(access_key, secret_key, region, 's3')
    
    # create aws ec2 resource
    aws_ec2_rs = main_upload.resource_aws(access_key, secret_key, region, 'ec2')

    # check if project name exists
    exists = main_upload.folder_exists(aws_s3_cl, opt.db, opt.project_name)
    
    if exists == True:
        print("[-] Failed to create new dir, project name ", str(opt.project_name), " already created, please use another name or delete project")
        exit

    # upload sequences file to to s3
    main_upload.upload_seqs_s3(opt.seqs, aws_s3_rs, opt.db, opt.project_name)

    # launch instance
    instance_id = main_upload.launch_instance_ami(aws_ec2_rs, ami_id_sqs, key_pair, sg_id, subnet_id, aws_ec2_cl, type)

    # CONNECT to instance
    print("[*] Waiting for port 22")
    time.sleep(25)
    print("[*] .")
    time.sleep(15)
    print("[*] ..")
    time.sleep(10)
    print("[*]...")

    # connect to instance and upload seqs into s3
    main_upload.execute_pipe(aws_ec2_cl, ssh_key_file, opt.project_name, opt.db, instance_id, bucket_sqs)

    # terminate instance
    main_upload.terminate_instance(aws_ec2_rs, instance_id)

# main function to upload seqs from local
def main_uploads2():
    print("[*] Local Upload to S3")
    opt = get_arguments()
    # function upload sequences
    directory = opt.seqs
    # load configuration file
    aws_conf = main_upload.load_config()
    # fetch params
    db = opt.db
    pName = opt.project_name
    bucket_name = aws_conf['aws']['s3_bucket_seqs']['name']
    access_key = aws_conf['aws']['credentials']['access_key']
    secret_key = aws_conf['aws']['credentials']['secret_key']
    region     = aws_conf['aws']['credentials']['region']
    # create aws s3
    s3 = main_upload.client_aws(access_key, secret_key, region, 's3')
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

# main function to execute nextflow
def main_pipes():
    # get arguments
    opt = get_arguments()

    # load aws-config file
    aws_conf = main_pipe.load_config()

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
    ec2_res = main_pipe.resource_ec2(access_key, secret_key,region)
    ec2_clt = main_pipe.client_ec2(access_key, secret_key,region)

    # EXECUTE AMI LAUNCH
    instance_id = main_pipe.launch_instance_ami(ec2_res, ami_ID, key_pair, sg_id, subnet_id,ec2_clt, type)

    
    # CONNECT to instance
    print("[*] Waiting for port 22")
    time.sleep(25)
    print("[*] .")
    time.sleep(15)
    print("[*] ..")
    time.sleep(10)
    print("[*]...")
    main_pipe.execute_pipe(ec2_clt, ssh_key_file , opt.project_name, opt.link , opt.db, instance_id)
    
    # TERMINATE Instance
    main_pipe.terminate_instance(ec2_res, instance_id)

# main function to orchestrate pipe
def main():
    # get arguments
    opt = get_arguments()
    code = int(opt.code)
    isFile = os.path.isfile(opt.seqs)
    # execute
    if code == 1:
        print("[x] Upload Workflow")
        if isFile:
            main_uploads()
        else:
            main_uploads2()
    elif code == 2:
        print("[x] Upload + Run Workflow")
        if isFile:
            main_uploads()
            main_pipes()
        else:
            main_uploads2()
            main_pipes()
    elif code == 3:
        print("[x] Run Workflow")
        main_pipes()
    else:
        print("[-] Specify 1 or 2 for Code Parameter")


####################
## Call functions ##
####################

if __name__ == "__main__":
    main()