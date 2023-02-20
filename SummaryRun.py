from utils._AOISummaryProduction_ import Summary_produce
from utils._TimeTracking_ import timeTracking
from utils._Main_Procedure_ import AOI_main_procedure
from utils._config_ import log_save_dir
import time
import logging
import pandas as pd
from SFTP.Connection import getFileAndTime, getLastBackUpTime
import schedule
import os
from utils._runBonding_ import bond_main
from datetime import datetime


sp = Summary_produce()
tt = timeTracking() 
def creatLogFile(filename):
    if not os.path.exists(log_save_dir):
        os.mkdir(log_save_dir)
        logging.basicConfig(
            filename= f'./log/{filename}', 
            filemode= 'w', 
            format= '%(asctime)s - %(message)s', 
            encoding= 'utf-8'
            )
    else:
        logging.basicConfig(
            filename= f'./log/{filename}', 
            filemode='a', 
            format='%(asctime)s - %(message)s',
            encoding='utf-8'
            )
            

def AOIrun(file_path):
    sheetIDDF_list = []
    try:
        for i in range(len(file_path)):
            print(f"Executing No.{i+1} File")
            file = file_path[i]
            AOI_CorresBond_SheetID_df = AOI_main_procedure(file)
            if len(AOI_CorresBond_SheetID_df.index) == 0: # empty dataframe
                logging.warning(f"[Not Found Warning] Can not found the '{file}' corresponding bond dataframe.")
            sheetIDDF_list.append(AOI_CorresBond_SheetID_df)
    except Exception as E:
        logging.error(f"[Error] line 49 Exception occurred: {str(E)}", exc_info=True)
    time.sleep(2)
    
    try:
        fullBondDF = pd.concat(sheetIDDF_list)
        if len(fullBondDF.index) == 0:
            logging.warning("[Warning] line52 No objects to concatenate")
        sp.save_summary_file(fullBondDF, sp.fullBondSummaryTableSP)
    except Exception as E:
        logging.error([f"line 58 fullBondDF error : {str(E)}"])


def AOImain():
    lastestBackupTime = getLastBackUpTime()
    data_list, fileTimeList = getFileAndTime(path1='/glob/data/path/*.csv')

    first_filtered_data = tt.filter_data_follow_CT(data_list, fileTimeList, "AOI", lastestBackupTime)
    if len(first_filtered_data)==0:
        logging.warning("[Warning] No AOI file in period of time.")
    else:
        try:
            first_filtered_data = sorted(first_filtered_data[:], key=lambda t: os.stat(t).st_mtime)
            second_filtered_data = sp.check_model_no(first_filtered_data)
            second_filtered_data = sorted(second_filtered_data, key=lambda t: os.stat(t).st_mtime)
        except Exception as E:
            logging.error(f"{str(E)}")
        
        sp.AOI_TimeRange(second_filtered_data)
        
        try:
            AOIrun(sorted(second_filtered_data))
        except Exception as E:
            logging.error(f"[Error] line 78 {str(E)}")

def main():
    creatLogFile(filename="running.log")
    logging.warning("Start Create Summary Table")
    bond_main()
    logging.warning("[INFO] AOI Start.")
    AOImain()
    logging.warning("Summary Table Created")

if __name__ == '__main__':
    main()

        

    
    


    
