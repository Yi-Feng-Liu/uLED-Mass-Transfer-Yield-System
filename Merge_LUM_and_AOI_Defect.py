import pandas as pd
import numpy as np
from pymongo import MongoClient
import pickle
import gridfs
from bson import ObjectId



class Merge_LUM_and_AOI_Defect():
    """Write the Yield 2D array, inspection type, LED type, decfect coordinate to mongoDB.
    """
    def __init__(self, SHEET_ID:str, key:str):
        super().__init__()
        
        # 判斷當前處理檔案的條件以及檢測條件
        self.client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017/?compressors=zlib')
        self.SHEET_ID = SHEET_ID # options
        self.Insepction_Type = key # options
        self.defect_df = pd.DataFrame()
        self.DB_name = 'MT'
        
        # got NG TFT info collection
        self.NG_TFT_Collection_name = 'COC2_AOI'
        self.NG_TFT_Collection_ng = 'COC2_AOI_INDEX'
        self.NG_TFT_Collection_arr = 'COC2_AOI_ARRAY'
        self.defect_code_dict = {
            'AB01': 'Particle',
            'AB02': 'Tilt',
            'AB03': 'Crack',
            'AB04': 'shift',
            'AB05': 'Defect Area',
            'AB06': '缺晶/Not Found',
            'AB07': 'LED色度',
            'AB08': 'LED缺晶',
            'AB09': 'LED亮/暗(輝度)',
            'AB10': 'Rotate',
            'AB11': 'PAD Loss',
            'AB12': 'On Dot',
            'AB13': 'Bright Dot',
            'AB14': 'Multi',
            'AB15': 'Edge',
            'OK': 'LED已上件'
        }


    def search_NG_LUM_InsType_dataframe(self):
        db = self.client[self.DB_name]
        collection = db['AOI_LUM_Defect_Coordinates']
        cursor = collection.find(
            { 'SHEET_ID': self.SHEET_ID, 'Insepction_Type': { '$in': self.Insepction_Type } },
            { '_id': 0 }
        )
        self.NG_LUM_InsType_df = pd.DataFrame.from_records(cursor)
        duplicate_cols = self.NG_LUM_InsType_df.columns.to_list()
        self.NG_LUM_InsType_df = self.NG_LUM_InsType_df.drop_duplicates(subset=duplicate_cols, keep='first').reset_index(drop=True)
        
        return self.NG_LUM_InsType_df
    
    
    def search_NG_LUM_defect_dataframe(self, OPID, CreateTime):
        self.OPID = OPID
        self.CreateTime = CreateTime # options
        db = self.client[self.DB_name]
        collection = db['AOI_LUM_Defect_Coordinates']
        cursor = collection.find(
            { 'SHEET_ID': self.SHEET_ID, 'Insepction_Type': { '$in': self.Insepction_Type }, 'OPID': { '$in': self.OPID }, 'CreateTime': { '$in': self.CreateTime } },
            { '_id': 0, 'AOI_OPID': 0, 'MAP': 0, 'Failure Analysis': 0, 'Solution': 0, 'Short term Action': 0, 'Defect': 0, 'Defect_Code': 0 }
        )
        
        self.NG_LUM_Defect_df = pd.DataFrame.from_records(cursor)
        
        return self.NG_LUM_Defect_df
    
    
    def get_NG_TFT_AOI_dataframe(self):
        db = self.client[self.DB_name]
        collection = db[self.NG_TFT_Collection_name]
        self.fs = gridfs.GridFS(db, collection=self.NG_TFT_Collection_ng)
        cursor = collection.find(
            { 'SHEET_ID': self.SHEET_ID }, 
            { '_id': 0, 'CreateTime': 1, 'SHEET_ID': 1, 'LED_TYPE': 1, 'OPID': 1, 'ng_info_id': 1 }
        )
       
        self.NG_TFT_AOI_df = pd.DataFrame.from_records(cursor)
        
        if len(self.NG_TFT_AOI_df.index) != 0:
            self.NG_TFT_AOI_df['CreateTime'] = self.NG_TFT_AOI_df['CreateTime'].astype(str)
            self.NG_TFT_AOI_df = self.NG_TFT_AOI_df.sort_values(by=['CreateTime'])
            self.NG_TFT_AOI_df = self.NG_TFT_AOI_df[self.NG_TFT_AOI_df['OPID'] != 'empty']
            return self.NG_TFT_AOI_df
        
        return pd.DataFrame()
    

    def search_NG_TFT_AOI_dataframe(self, OPID, CreateTime):
        db = self.client[self.DB_name]
        collection = db[self.NG_TFT_Collection_name]
        self.fs = gridfs.GridFS(db, collection=self.NG_TFT_Collection_ng)
        cursor = collection.find(
            { 'SHEET_ID': self.SHEET_ID, 'OPID': { '$in': OPID }, 'CreateTime': { '$in': CreateTime } },
            { '_id': 0, 'CreateTime': 1, 'SHEET_ID': 1, 'LED_TYPE': 1, 'OPID': 1, 'ng_info_id': 1 }
        )
        
        self.NG_TFT_AOI_df_filter = pd.DataFrame.from_records(cursor)
        
        if len(self.NG_TFT_AOI_df_filter.index) != 0:
            self.NG_TFT_AOI_df_filter['CreateTime'] = self.NG_TFT_AOI_df_filter['CreateTime'].astype(str)
            self.NG_TFT_AOI_df_filter = self.NG_TFT_AOI_df_filter.sort_values(by=['CreateTime'], ascending=False)
            self.NG_TFT_AOI_df_filter = self.NG_TFT_AOI_df_filter[self.NG_TFT_AOI_df_filter['OPID'] != 'empty']
            return self.NG_TFT_AOI_df_filter
        
        return pd.DataFrame()
            
        
    # def get_SHEET_ID_list(self, NG_TFT_AOI_df:pd.DataFrame):
    #     """Get the SHEET_ID of NG TFT AOI df
    #     """
    #     # 確認是否有這片SHEET ID
    #     self.get_NG_TFT_AOI_dataframe()
    #     self.TFT_AOI_SHEE_ID_list = []
    #     if len(NG_TFT_AOI_df.index) != 0:
    #         self.TFT_AOI_SHEE_ID_list = list(dict.fromkeys(NG_TFT_AOI_df['SHEET_ID'].tolist()))
        
        
    # def get_sheet_id_dataframe(self, NG_TFT_AOI_df:pd.DataFrame):
    #     """Step_1. Find correspong the dataframe from processing init info.
    #     """
    #     # self.get_SHEET_ID_list()
    #     self.TFT_sheet_df = pd.DataFrame()
    #     # 把 empty 的 opid 拿掉, 若有重測, 則選擇最新的資料
    #     if len(self.TFT_AOI_SHEE_ID_list) != 0:
    #         self.TFT_sheet_df = NG_TFT_AOI_df.sort_values(by=['CreateTime'], ascending=False)
    #         print(self.TFT_sheet_df, '\n')
            
    #         print(self.TFT_sheet_df)
    #     return self.TFT_sheet_df
                
    
    def get_bonding_dataframe(self):
        """Get bonding time from database MT and that collection BondSummaryTable
        
        The TFT createTime and processing createTime needs to in period of two bonding times.
        else, used the last time of bonding time to be the filter condiction.
        """
        
        db = self.client["MT"]
        collection = db["BondSummaryTable"]
        
        # 在 bonding 的 dataframe 中，找到目前正在處理的 sheet ID 是否存在於 bonding dataframe。
        cursor = collection.find(
            { 'SHEET_ID': self.SHEET_ID, 'CreateTime': { '$in': self.CreateTime } }, 
            { 'CreateTime': 1, 'SHEET_ID': 1, 'LED_TYPE': 1 }
        )
        
        bonding_df = pd.DataFrame.from_records(cursor)
        
        if isinstance(bonding_df, type(None)):
            bonding_df = pd.DataFrame()
            
        if len(bonding_df.index) != 0:
            bonding_df['CreateTime'] = bonding_df['CreateTime'].astype(str)
            bonding_df = bonding_df.sort_values(by=['CreateTime'], ascending=False).reset_index(drop=True)
            # 刪除重複的資料
            bonding_df = bonding_df.drop_duplicates(['CreateTime', 'SHEET_ID', 'LED_TYPE'], keep='first').reset_index(drop=True)
        else:
            print(f'[WARNING] COLLECTION: BondSummaryTable NOT FOUND SHEET ID {self.SHEET_ID}')
        
        return bonding_df
        
        
    def compare_createTime(self, df):
        """Step_2. Compare the createTime from TFT_sheet_df, then get the TFT defect coordinates and image url.
        """
        
        bond_sheet_df = self.get_bonding_dataframe()
        self.TFT_time_df = pd.DataFrame()
        max_time = max(self.CreateTime)
        min_time = min(self.CreateTime)
        
        # 如果沒有bonding的資料 就找大於或小於 目前處理檔案的時間
        if len(bond_sheet_df.index) == 0:
            if len(df.index) != 0:
                self.TFT_time_df = df
                return self.TFT_time_df
        else:
            time_ls = sorted(bond_sheet_df["CreateTime"].astype(str).tolist()) 
            del bond_sheet_df
            
            # 判斷 TFT 的 sheet ID dataframe 是否介於兩次 bonding 之間，或者大於第一次 bonding 時間。
            if len(df.index) != 0:
                # 確認 TFT_sheet_df 有哪些資料在兩次的 bonding 之間
                for previous, current in zip(time_ls, time_ls[1:]):
                    self.TFT_time_df = df[(df['CreateTime'].astype(str) < current) & (df['CreateTime'].astype(str) > previous)]
                        
                    if len(self.TFT_time_df.index) != 0:
                        return self.TFT_time_df
                    
                    else:
                        # 如果沒有，則選擇大於第一次 bonding 時間，因為AOI和LUM的檢查順序不一定，所以選擇or判斷
                        self.TFT_time_df = df[(df['CreateTime'].astype(str) > previous)]
                        self.TFT_time_df = self.TFT_time_df[(self.TFT_time_df['CreateTime'].astype(str) < max_time) | (self.TFT_time_df['CreateTime'].astype(str) > min_time)]
                        return self.TFT_time_df
            else:
                print('Not Found correspond sheet dataframe in line 891')
                
       
    
    def merge_LUM_AOI_dataframe(self, df_lum, df_aoi):
        """Get image_url by TFT defect position and LUM defect coordinates
        """
        
        self.TFT_time_df = self.compare_createTime(df_aoi) 
        if isinstance(self.TFT_time_df, type(None)):
            self.TFT_time_df = pd.DataFrame()
        
        tft_x_coord, tft_y_coord, tft_img_link, tft_defect_code = [], [], [], []
        OPID_ls = []
        type_ls = []
        self.defect_df = df_lum.reset_index(drop=True).copy()
        
        if len(self.TFT_time_df.index) != 0:
            AOI_OPID_ls = list(dict.fromkeys(self.TFT_time_df['OPID'].tolist()))
            led_type_ls = list(dict.fromkeys(self.TFT_time_df['LED_TYPE'].tolist()))
            
            # 每個 OPID 都有 ng_info_id, 所以得找出特定的 dataframe
            for led_type in led_type_ls:
                for OPID in AOI_OPID_ls:
                    OPID_df = self.TFT_time_df[(self.TFT_time_df['OPID']==OPID) & (self.TFT_time_df['LED_TYPE']==led_type)]
                    ng_series = OPID_df.ng_info_id.reset_index(drop=True)
                    for i in range(len(ng_series.index)):
                        each_row_dict_id = ng_series[i]
                        dic = self.fs.get(ObjectId(each_row_dict_id)).read()
                        infos = pickle.loads(dic)
                        for j in range(len(infos)):
                            tft_x_coord.append(infos[j].get('LED_Index_X'))
                            tft_y_coord.append(infos[j].get('LED_Index_Y'))
                            tft_defect_code.append(infos[j].get('Defect Reciepe'))
                            tft_img_link.append(infos[j].get('LINK'))
                            OPID_ls.append(OPID) 
                            type_ls.append(led_type)  
                                 
            temp_tft_df = pd.DataFrame(columns=['AOI_OPID', 'LED_TYPE', 'Pixel_X', 'Pixel_Y', 'MAP'])
            temp_tft_df['AOI_OPID'] = OPID_ls
            temp_tft_df['LED_TYPE'] = type_ls
            temp_tft_df['Pixel_X'] = tft_x_coord
            temp_tft_df['Pixel_Y'] = tft_y_coord
            temp_tft_df['MAP'] = tft_img_link
            temp_tft_df['Defect_Code'] = tft_defect_code
            temp_tft_df = temp_tft_df[temp_tft_df['Defect_Code']!='BA0X']
            
            # drop_duplicates 避免merge的時候 dataframe 被展開，導致對不上 defect 長度
            temp_tft_df.drop_duplicates(subset=['AOI_OPID', 'LED_TYPE', 'Pixel_X', 'Pixel_Y'], keep='last', inplace=True)
            
            new_cols = []
            for OPID in AOI_OPID_ls:
                temp_tft_df[OPID + '_MAP'] = np.where(temp_tft_df['AOI_OPID']==OPID, temp_tft_df['MAP'], '')
                new_cols.append(OPID + '_MAP')
                
            temp_tft_df['CK'] = 1
            tft_cols = temp_tft_df.columns.to_list()
            tft_cols.remove('CK')
            
            temp_tft_df_pivot = pd.pivot_table(temp_tft_df, values='CK', index=tft_cols, columns=['AOI_OPID']).reset_index()
            tft_df_pivot_cols = temp_tft_df_pivot.columns.to_list()
            tft_cols.remove('MAP')
  
            for opid_map in new_cols:
                tft_df_pivot_cols.remove(opid_map)
                
            for AOI_OPID in AOI_OPID_ls:
                temp_tft_df_pivot[AOI_OPID] = np.where(
                    temp_tft_df_pivot[AOI_OPID]==1, 
                    temp_tft_df_pivot['Defect_Code'].apply(lambda x: self.defect_code_dict.get(x) if x != np.nan else ''), 
                    ''
                )
                
            temp_tft_df_group = temp_tft_df_pivot.groupby(by=['LED_TYPE', 'Pixel_X', 'Pixel_Y']).agg(sum).reset_index()      
            temp_tft_df_group = temp_tft_df_group.groupby(tft_df_pivot_cols)[new_cols].agg(sum).reset_index()
            temp_tft_df_group.drop(['AOI_OPID', 'Defect_Code', 'MAP'], axis=1, inplace=True)
            defect_df1 = self.defect_df.merge(temp_tft_df_group, how='outer', on=['LED_TYPE', 'Pixel_X', 'Pixel_Y'])
            defect_df1 = defect_df1.dropna(subset=['Luminance'])
            
            defect_df1['CK'] = 1
            defect_df1_cols = defect_df1.columns.to_list()
            defect_df1_cols.remove('CK')
            
            defect_df1_pivot = pd.pivot_table(defect_df1, values='CK' ,index=defect_df1_cols, columns=['OPID']).reset_index()
            LUM_OPID_ls = list(dict.fromkeys(defect_df1_pivot['OPID'].tolist()))
            defect_df1_pivot.drop(['OPID'], axis=1, inplace=True)
            defect_df1_pivot_cols = defect_df1_pivot.columns.to_list()
            
            
            for LUM_OPID in LUM_OPID_ls:
                defect_df1_pivot_cols.remove(LUM_OPID)
      
            for i in range(len(LUM_OPID_ls)):
                OPID = LUM_OPID_ls[i]
                defect_df1_pivot[OPID] = np.where(
                    defect_df1_pivot[OPID]==1, 'LED 亮/暗(輝度)', ''
                )
               
            defect_df1_pivot.drop(['Luminance'], axis=1, inplace=True)
            defect_df1_pivot_cols.remove('Luminance')
            
            defect_df1_pivot_cols.remove('CreateTime') # 避免時間的不同會讓column合併不起來
            defect_df1_group = defect_df1_pivot.groupby(defect_df1_pivot_cols)[LUM_OPID_ls].agg(sum).reset_index() 
            defect_df1_group_col = defect_df1_group.columns.to_list()
            
            # reorder the col list
            fixed_col_limit = defect_df1_group_col.index('Pixel_Y')
            fixed_col = []
            for col in range(fixed_col_limit+1):
                fixed_col.append(defect_df1_group_col[col]) 
                  
            # append OPID list and group them      
            standard_squence = ['BO', 'DE', 'RE']
            for ss in standard_squence:
                for LUM_OPID in LUM_OPID_ls:
                    if LUM_OPID.endswith(ss):
                        fixed_col.append(LUM_OPID)
                        for AOI_OPID in AOI_OPID_ls:
                            if AOI_OPID[-2:] == LUM_OPID[-2:]:
                                fixed_col.append(AOI_OPID)
                                fixed_col.append(AOI_OPID + '_MAP')
                        
            defect_df1_group = defect_df1_group[fixed_col]
            
            # 最後一個AOI站點的原因
            defect_df1_group['Failure Analysis'] = defect_df1_group[fixed_col[-2]]
            defect_df1_group['Solution'] = ''
            defect_df1_group['Short term Action'] = ''
            
            return defect_df1_group
        
        else:
            
            self.defect_df['AOI_OPID'] = ["" for _ in range(len(self.defect_df.index))]
            self.tft_img_link = ["" for _ in range(len(self.defect_df.index))]
            self.defect_df['MAP'] = self.tft_img_link
            self.defect_df['Defect_Code'] = ["" for _ in range(len(self.defect_df.index))]
            print('[INFO] self.TFT_time_df is None')
            
            return self.defect_df
        
        
