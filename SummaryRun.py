from utils._AOISummaryProduction_ import Summary_produce, csv_pre_filter
from utils._TimeTracking_ import timeTracking
from utils._Main_Procedure_ import AOI_main_procedure
from utils._config_ import log_save_dir
import time
import logging
import pandas as pd
from SFTP.Connection import getFileAndTime
import schedule
import os
from utils._runBonding_ import bond_main
from datetime import datetime
import uuid
# import psutil



temp_aoi_time_range_csv_name = 'AOI_Summary.csv'
def creatLogFile(filename):
    path = f'{log_save_dir}{filename}'
    if not os.path.exists(log_save_dir):
        os.mkdir(log_save_dir)
        
    logging.basicConfig(
        filename= path, 
        filemode= 'a', 
        format= '%(asctime)s - %(message)s', 
        encoding= 'utf-8')
    
    

def string_to_datetime(strtime:str):
    """Change the string time format to datetime object

    Parameter:
    ----------
        strtime (str): 2023-05-30 13:50:53.72333 as string will be change into datetime object

    Returns:
        datetime.strptime
    """
    return datetime.strptime(strtime, "%Y-%m-%d %H:%M:%S.%f")


def AOIrun(file_path):
    sp = Summary_produce()
    sheetIDDF_list = []
    fullBondDF = pd.DataFrame()
    # memory_usage = psutil.virtual_memory()
    for i in range(len(file_path)):
        # logging.warning('Record:\n')
        # logging.warning(f'CPU: CPU percent {psutil.cpu_percent()}, Count: {psutil.cpu_count}')
        # logging.warning(f'Memory: {memory_usage.used}, used percent: {memory_usage.percent}')
        print(f"Executing No.{i+1} File")
        file = file_path[i]
        for key in sp.inspection_type_list:
            AOI_CorresBond_SheetID_df = AOI_main_procedure(file, key, temp_aoi_time_range_csv_name)
            if len(AOI_CorresBond_SheetID_df.index) == 0: # empty dataframe
                logging.warning(f"[Not Found Warning] Can not found the '{file}' corresponding bond dataframe.")
            sheetIDDF_list.append(AOI_CorresBond_SheetID_df)
    
    # 因有不同的檢測結果, MongoDB的id有可能會重複, 為了避免這種情況, 使用 uuid.uuid4() 製作每個row的專屬id, 以確保不重複。
    fullBondDF = pd.concat(sheetIDDF_list)
    fullBondDF.drop('_id', axis=1, inplace=True)
    fullBondDF['_id'] = [str(uuid.uuid4()) for _ in range(len(fullBondDF))]
    # fullBondDF = fullBondDF.drop_duplicates(['_id'], keep='last').reset_index(drop=True)
    if len(fullBondDF.index) == 0:
        logging.warning("[Warning] line52 No objects to concatenate")     
    else:
        sp.insert_dataframe_to_mongoDB(whole_df=fullBondDF, collection_name='LUM_Bond_SummaryTable')
        print('FullBondSummaryTable inserted')
    del fullBondDF, sheetIDDF_list
    # except Exception as E:
    #     logging.error([f"line 56 fullBondDF error : {str(E)}"])


def AOImain(key_name:str, past_time:str|None):
    """choose patch data or not

    Args:
        key_name (str): AOI or Past_AOI
        
        past_time (str | None): past_time is a threshold that used to set a range for period of time of files.
    """
    # past_time 指的是到時間範圍的上限, 起始點需要到Past_AOI_timingRange.json去設置
    tt = timeTracking()
    data_list, fileTimeList = getFileAndTime(
        './UM/UMAOI100/DATA/UPLOAD/HIS/*LUM_EDC.csv', 
        # '/UM100/UMAOI100/DATA/UPLOAD/NG/*LUM_EDC.csv', # REAL & NG 內的 LUM 檔案不完整
        # '/UM100/UMAOI100/DATA/UPLOAD/REAL/*LUM_EDC.csv'
    )
    if key_name=="AOI":
        first_filtered_data = tt.filter_data_follow_CT(data_list[:], fileTimeList[:], key_name, datetime.now())
    elif key_name=="Past_AOI":
        logging.warning("[INFO] Start Patch AOI data")
        past_time = string_to_datetime(past_time)
        first_filtered_data = tt.filter_data_follow_CT(data_list[:], fileTimeList[:], key_name, past_time)
    del data_list, fileTimeList
    
    
    if len(first_filtered_data)==0:
        logging.warning("[Warning] No AOI file in period of time.")
    else:
        logging.warning("[INFO] Start Create LUM Summary Table")
        filtered_data = sorted(first_filtered_data, key=lambda t: os.stat(t).st_mtime)
        logging.warning(f"[INFO] It have {len(filtered_data)} files in total.")
        filtered_data = csv_pre_filter(filtered_data[:], temp_aoi_time_range_csv_name).AOI_TimeRange()
        AOIrun(filtered_data)
    logging.warning("[INFO] Done")


def main():
    bond_main()
    AOImain(key_name="AOI", past_time=None)
    


if __name__ == '__main__':

    # schedule.every().hours.at("25:00").do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(5)
        
    main()

    
    


    
