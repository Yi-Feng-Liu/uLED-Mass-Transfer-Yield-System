# import paramiko
from datetime import datetime
from glob import glob
import os

# Deprecated
# def connectSSH(hostname: str, port: int, username: str, password: str, path: str):
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     ssh.connect(hostname=hostname, port=port, username=username, password=password)
#     sftp_client = ssh.open_sftp() 
#     directory_list = sftp_client.listdir(path)
#     return sftp_client, directory_list


def getFileAndTime(*args:str):
    """Input the file paths to extract. 
    
    args support glob path form

    Returns:
        list: all_file
        list: all_file_time_ls
    """
    all_file = []
    all_file_time_ls = []
    for i in args:
        files = glob(i)
        filesTimels = [datetime.fromtimestamp(os.stat(i).st_mtime) for i in files]
        all_file += files
        all_file_time_ls += filesTimels
    return all_file, all_file_time_ls

    
  

