from utils._AOISummaryProduction_ import csv_pre_filter
from utils._TimeTracking_ import timeTracking
import logging
from SFTP.Connection import getFileAndTime
import schedule
import os
from datetime import datetime
import time



def string_to_datetime(strtime: str) -> datetime:
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


def light_On_main(executed_type: str):
    """choose patch data or not

    Args:
        executed_type (str): light_on or Past_light_on
    """
    tt = timeTracking()
    csv_filter = csv_pre_filter()
    
    data_list, fileTimeList = getFileAndTime(
        # './UM/UMAOI100/DATA/UPLOAD/HIS/*LUM_EDC.csv',
        # '/UM/UMLIT200/DATA/UPLOAD/HIS/*LUM_EDC.csv',
        './UM/UMAOI100/DATA/UPLOAD/HIS/*DMU_EDC.csv',
    )
    first_filtered_data = tt(types=executed_type, datalist=data_list[:], dataCTlist=fileTimeList[:], timer=datetime.now())
    
    del data_list, fileTimeList
    
    
    if len(first_filtered_data)==0:
        logging.warning("[INFO] No light_on file in period of time.")
        
    else:
        logging.warning(f"[INFO] Start Create LUM Summary Table")
        filtered_data = sorted(first_filtered_data, key=lambda t: os.stat(t).st_mtime)
        logging.warning(f"[INFO] Discover {len(filtered_data)} files in total.")
        csv_filter(filtered_data)



def main():
    light_On_main(executed_type="light_on")
    


if __name__ == '__main__':

    # schedule.every().hours.at("40:00").do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(5)
        
    main()

    
    


    
