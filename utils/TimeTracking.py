from datetime import datetime
import json
import os
from typing import Any


class timeTracking():
    """__call__ function will return filtered data list by record time
    
    Params:
    --------
    
    types(str): Used to decide file name of json
    
    datalist(list): list of data path
    
    dataCTlist(list): list of creat time from data list
    
    timer(datetime): datetime.now() or datetime format
    """
    
    
    def __call__(self, types:str, datalist:list, dataCTlist:list, timer:datetime) -> list:
        self.types = types
        self.datalist = datalist
        self.dataCTlist = dataCTlist
        self.timer = timer
        filter_data_ls = self.filter_data_follow_CT()
        return filter_data_ls


    def start_time(self, fileName, recordtime=None):
        json_time_object = {self.types : recordtime}
        timeToJson = json.dumps(json_time_object, default=str)
        with open(fileName, 'w') as out2file:
            out2file.write(timeToJson)


    def read_last_record_time(self, fileName):
        with open(fileName, 'r') as openfile:
            record_file = json.load(openfile)
        last_time = datetime.fromisoformat(record_file[self.types])
        return last_time 
    
    
    def get_filter_data_ls(self, reocrd_time_json_file:str) -> list:
        filtered_data_list = []
        if not os.path.exists(reocrd_time_json_file):
            self.start_time(reocrd_time_json_file, recordtime=self.timer)
            initial_time = self.timer.replace(year=2022, month=10, day=1, hour=0, minute=0, second=0)
            
            for file, filetime in zip(self.datalist, self.dataCTlist):
                if filetime < self.timer and filetime > initial_time:
                    filtered_data_list.append(file)
            return filtered_data_list
        
        else:
            last_time = self.read_last_record_time(reocrd_time_json_file)
            self.start_time(reocrd_time_json_file, recordtime=self.timer)
            
            for file, filetime in zip(self.datalist, self.dataCTlist):
                if filetime < self.timer and filetime > last_time:
                    filtered_data_list.append(file)
            return filtered_data_list


    def filter_data_follow_CT(self):
        print(f"[INFO] Filter {self.types} data from Create Time...")
        filtered_data_list = self.get_filter_data_ls(f'Run_{self.types}_TimingRecord.json')
        return filtered_data_list
