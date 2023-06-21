import paramiko
from datetime import datetime
from glob import glob
import os
import logging


def connectSSH(hostname: str, port: int, username: str, password: str, path: str):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, port=port, username=username, password=password)
    sftp_client = ssh.open_sftp() 
    directory_list = sftp_client.listdir(path)
    return sftp_client, directory_list


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

    
def getLightOnResult(sheetIDls, MODEL):
    sftp_client, directory_list= connectSSH(
        hostname="L4AFLS01", 
        port=22, 
        username="wma", 
        password="wma", 
        path='/home/nfs/ledimg/UMAOI100/LUMIMG/',
    )
    key_model_path_dict = {
        13.6: './report_production/13.6/',
        16.1: './report_production/16.1/',
        17.3: './report_production/17.3/'
    }
    found_sheet = []
    for sheetID in sheetIDls:
        folderls = []   
        for i in directory_list:
            if i.split("_")[0] == 'LUM':
                foldername = i.split("_")[1]
                if foldername == sheetID:
                    folderls.append(i)
                    found_sheet.append(sheetID)
            else:
                continue
        
        total_folder = folderls
        total_folder = sorted(total_folder)
        # print(total_folder)
        if len(total_folder) == 0:
            logging.warning(f"No correspong sheet id {sheetID} of image")
            
        else:
            the_up_to_date_folder = total_folder[-1]
            # print(the_up_to_date_folder)
            img_path = f'/home/nfs/ledimg/UMAOI100/LUMIMG/{the_up_to_date_folder}/Result/'
            # print(img_path)
            try:
                imgls = [file for file in sftp_client.listdir(img_path) if file.endswith(".bmp") if file.startswith("DefectMap") or file.startswith("ResultForm")]
                local_path = key_model_path_dict.get(MODEL)
                for img in imgls:
                    sftp_client.get(f"{img_path+img}", f"{local_path+img}")
                    imgName = local_path+img
                    imgName = imgName.split("/")[-1]
                    os.replace(local_path+img, f"{local_path + sheetID}_{imgName}")
            except Exception as E:
                logging.warning(f"Line 91 has erroe \n str{E} \nin Connection.py")
    try:
        # take the element out and that not in intersection list.
        # the len of found sheet greater than original sheet ID list is impossible
        lost_img = []
        found = list(dict.fromkeys(found_sheet))
        total_sheet = sheetIDls
        if len(found) <= len(total_sheet) and len(found) > 0:
            for i in total_sheet:
                if i not in found:
                    lost_img.append(i)
        if len(list(set(found_sheet))) == 0:
            lost_img = sheetIDls      
    except Exception as E:
        logging.warning(f"Connection.py at line 116 had warning {E}")

    return lost_img
    
    
  

