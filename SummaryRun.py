from utils._AOISummaryProduction_ import Summary_produce, csv_pre_filter
from utils._TimeTracking_ import timeTracking
from utils._Main_Procedure_ import AOI_main_procedure
import logging
import pandas as pd
from SFTP.Connection import getFileAndTime
import schedule
import os
from utils._runBonding_ import repair_bonding_main
from datetime import datetime
import uuid
import psutil
from time import time


def string_to_datetime(strtime:str) -> datetime:
    """Change the string time format to datetime object

    Parameter:
    ----------
    strtime (str): string will be change into datetime object
    
    >>> date = string_to_datetime('2023-03-30 13:50:53.72333')
    >>> date
    2023-03-30 13:50:53.72333

    Returns:
        datetime.strptime
    """
    return datetime.strptime(strtime, "%Y-%m-%d %H:%M:%S.%f")



def AOIrun(file_path, temp_AOI_sheet_time_df):
    sp = Summary_produce()
    sheetIDDF_list = []
    fullBondDF = pd.DataFrame()
    memory_usage = psutil.virtual_memory()
    
    for i in range(len(file_path)):
        logging.warning('Record:\n')
        logging.warning(f'CPU: CPU percent {psutil.cpu_percent()}, Count: {psutil.cpu_count}')
        logging.warning(f'Memory: {memory_usage.used}, used percent: {memory_usage.percent}')
        
        print(f"Executing No.{i+1} File")
        
        file = file_path[i]
        s = time()
        for key in sp.inspection_type_list:
            AOI_CorresBond_SheetID_df = AOI_main_procedure(file, key, temp_AOI_sheet_time_df)
            if len(AOI_CorresBond_SheetID_df.index) != 0: # empty dataframe
                sheetIDDF_list.append(AOI_CorresBond_SheetID_df)
        print(f'Process No.{i+1} file cost {(time() - s)/60:.4f} mins')
        
        
    # 因有不同的檢測結果, MongoDB的id有可能會重複, 為了避免這種情況, 使用 uuid.uuid4() 製作每個row的專屬id, 以確保不重複。
    try:
        fullBondDF = pd.concat(sheetIDDF_list)
        fullBondDF.drop('_id', axis=1, inplace=True)
        fullBondDF['_id'] = [str(uuid.uuid4()) for _ in range(len(fullBondDF))]
    except:
        pass
    
    if len(fullBondDF.index) != 0:
        sp.insert_dataframe_to_mongoDB(whole_df=fullBondDF, collection_name='LUM_Bond_SummaryTable')
        print('FullBondSummaryTable inserted')
        
    del fullBondDF, sheetIDDF_list


def light_On_main(executed_type:str):
    """choose patch data or not

    Args:
        executed_type (str): light_on or Past_light_on
    """
    tt = timeTracking()
    csv_filter = csv_pre_filter()
    
    
    data_list, fileTimeList = getFileAndTime(
        './file/path/test/*.csv',
    )
    
    first_filtered_data = tt(types=executed_type, datalist=data_list[:], dataCTlist=fileTimeList[:], timer=datetime.now())

    del data_list, fileTimeList
    
    
    if len(first_filtered_data)==0:
        logging.warning("[Warning] No light_on file in period of time.")
        
    else:
        logging.warning("[INFO] Start Create LUM Summary Table")
        
        filtered_data = sorted(first_filtered_data[:], key=lambda t: os.stat(t).st_mtime)
        logging.warning(f"[INFO] Discover {len(filtered_data)} files in total.")
        
        filtered_data = csv_filter(filtered_data[:], temp_aoi_time_range_csv_name)
        AOI_TimeRange_df = pd.read_csv(f"./{temp_aoi_time_range_csv_name}", dtype=str)
        AOIrun(filtered_data, AOI_TimeRange_df)
        
    logging.warning("[INFO] Done")


def main():
    repair_bonding_main()
    light_On_main(executed_type="light_on")
    


if __name__ == '__main__':
    
    temp_aoi_time_range_csv_name = 'light_on_Summary.csv'
    
    # schedule.every().hours.at("25:00").do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(5)
        
    main()

    
    


    
