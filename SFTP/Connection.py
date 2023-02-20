import paramiko
from datetime import datetime
from glob import glob
import os


def connectSSH(hostname: str, port: int, username: str, password: str, path: str, path2: str|None=None, endswith:str=None):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, port=port, username=username, password=password)
    sftp_client = ssh.open_sftp() 

    directory_list = sftp_client.listdir(path)

    # filteredList = [path + file for file in directory_list if file.endswith(endswith)]

    # if path2 != None:
    #     directory_list2 = sftp_client.listdir(path2)
    #     filteredList2 = [path2 + file for file in directory_list2 if file.endswith(endswith)]
    #     totalFileList = filteredList + filteredList2
    #     totalFileTimeList = [datetime.fromtimestamp(sftp_client.stat(i).st_mtime) for i in totalFileList]
    #     return sftp_client, directory_list, totalFileList, totalFileTimeList

    # fileTimeList = [datetime.fromtimestamp(sftp_client.stat(i).st_mtime) for i in directory_list]

    return sftp_client, directory_list, #fileTimeList, fileTimeList


def getLastBackUpTime(path="./programe/backuplog.txt"):
    lastTime = []
    with open(path, "r", encoding="utf-8") as f:
        for _, line in enumerate(f):
            lastTime.append(line.split("[")[0])
    lastTIME = lastTime[-1].replace('/', '-').strip() # Remove Trailing Spaces
    return datetime.fromisoformat(lastTIME)


def getFileAndTime(path1: str, path2=None):
    files = glob(path1)
    filesTimels = [datetime.fromtimestamp(os.stat(i).st_mtime) for i in files]
    if path2 != None:
        files2 = glob(path2)
        totalFilels = files + files2
        totalFileTimels = [datetime.fromtimestamp(os.stat(i).st_mtime) for i in totalFilels]
        return totalFilels, totalFileTimels
    return files, filesTimels
    
def getLightOnResult(sheetIDls):
    sftp_client, directory_list= connectSSH(
        hostname="hostname", 
        port=22, 
        username="username", 
        password="password", 
        path='/path/of/data/',
        endswith="_report.csv"
    )
    
    for sheetID in sheetIDls:
        folderls = []
        otherFolder = []
        
        for i in directory_list:
            if i.split("_")[0] == 'LUM':
                foledername = i.split("_")[1]
                if foledername == sheetID:
                    folderls.append(i)

            if i.split("_")[0] != 'LUM':
                if i.split("_")[0] == sheetID:
                    otherFolder.append(i)
            else:
                continue

        total_folder = folderls + otherFolder
        if len(total_folder) != 0:
            total_folder = sorted(total_folder)
     
            the_up_to_date_folder = total_folder[-1]
            img_path = f'/home/nfs/ledimg/UMAOI100/LUMIMG/{the_up_to_date_folder}/Result/'
            imgls = [file for file in sftp_client.listdir(img_path) if file.endswith(".bmp") if file.startswith("DefectMap") or file.startswith("ResultForm")]

            local_path = './report_production/'
            for img in imgls:
                sftp_client.get(f"{img_path+img}", f"{local_path+img}")
                imgName = local_path+img
                imgName = imgName.split("/")[-1]
                os.replace(local_path+img, f"{local_path + sheetID}_{imgName}")
            
            
    

if __name__ == "__main__":

    local_img_ls = getLightOnResult("9702K1")
    # print(local_img_ls)
    
  

