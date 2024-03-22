import pandas as pd
import numpy as np
from pymongo import MongoClient
import pickle
import gridfs
from bson import ObjectId
import json
import streamlit as st



class Merge_LUM_and_AOI_Defect():
    """Merge Light_on's and AOI's dataframe from MongoDB."""
    
    def __init__(self, SHEET_ID: str, Inspection_Type: str):
        super().__init__()
        
        # 判斷當前處理檔案的條件以及檢測條件
        self.client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017/?compressors=zlib')
        self.SHEET_ID = SHEET_ID # options
        self.Inspection_Type = Inspection_Type # options
        self.DB_name = 'MT'
        
        # got TFT AOI info
        self.TFT_AOI_Collection = 'COC2_AOI'
        self.TFT_AOI_DF_Collection = 'COC2_AOI_DF'
    
        config_file = open("./config.json", "rb")
        self.config = json.load(config_file)
        self.defect_code_dict = self.config['defect_code_dict']
        self.opid_dict = self.config['opid_dict']
        
        self.AOIfs = self.GridFS(collection_name = self.TFT_AOI_DF_Collection)
        self.LUMfs = self.GridFS(collection_name = 'LUM_SummaryTable')
        
        
    def query_LUM_dataframe(self, OPID: str|None=None, CreateTime: str|None=None) -> pd.DataFrame:
        db = self.client[self.DB_name]
        collection = db['LUM_SummaryTable']
        LUM_query_field = {
            '_id': 0, 
            'CreateTime': 1, 
            'SHEET_ID': 1, 
            'OPID': 1, 
            'LED_TYPE': 1, 
            'Inspection_Type': 1,
            'Dataframe_id': 1 ,
        }
        
        if OPID != None and CreateTime != None:
            cursor = collection.find(
                {
                    'SHEET_ID': self.SHEET_ID, 
                    'Inspection_Type': self.Inspection_Type, 
                    'OPID': {'$in': OPID}, 
                    'CreateTime': {'$in': CreateTime} 
                },
                LUM_query_field
            )
        else:
            cursor = collection.find(
                {
                    'SHEET_ID': self.SHEET_ID,
                    'Inspection_Type': self.Inspection_Type,
                },
                LUM_query_field
            )
        
        LUM_InsType_df = pd.DataFrame.from_records(cursor)
        if len(LUM_InsType_df.index) != 0:
            duplicate_cols = LUM_InsType_df.columns.to_list()
            LUM_InsType_df = LUM_InsType_df.drop_duplicates(subset=duplicate_cols, keep='first').reset_index(drop=True)
            return LUM_InsType_df
        
        return pd.DataFrame()
    
    
    def GridFS(self, collection_name: str) -> gridfs.GridFS:
        db = self.client[self.DB_name]
        fs = gridfs.GridFS(db, collection_name)
        
        return fs
    
    
    def query_TFT_AOI_dataframe(self, OPID: str|None=None, CreateTime: str|None=None) -> pd.DataFrame:
        db = self.client[self.DB_name]
        collection = db[self.TFT_AOI_Collection]
        AOI_query_field = {
            '_id': 0, 
            'CreateTime': 1,
            'SHEET_ID': 1, 
            'LED_TYPE': 1, 
            'OPID': 1, 
            'df_id': 1 
        }
        
        if  OPID != None and CreateTime != None:
            cursor = collection.find(
                {
                    'SHEET_ID': self.SHEET_ID, 
                    'OPID': {'$in': OPID}, 
                    'CreateTime': {'$in': CreateTime} 
                },
                AOI_query_field
            )
        else:
            cursor = collection.find(
                {
                    'SHEET_ID': self.SHEET_ID 
                }, 
                AOI_query_field
            )
        
        TFT_AOI_df = pd.DataFrame.from_records(cursor)
        
        if len(TFT_AOI_df.index) != 0:
            TFT_AOI_df['CreateTime'] = TFT_AOI_df['CreateTime'].astype(str)
            TFT_AOI_df = TFT_AOI_df.sort_values(by=['CreateTime', 'LED_TYPE'], ascending=False)
            TFT_AOI_df = TFT_AOI_df[TFT_AOI_df['OPID'] != 'empty']
            return TFT_AOI_df
        
        return pd.DataFrame()
    
    
    def getDataframeFromObjectID(self, fs, objectID: ObjectId) -> pd.DataFrame:
        df = fs.get(ObjectId(objectID)).read()
        df = pickle.loads(df)
        
        return pd.DataFrame(df)
    
    
    def getCTDataframeDict(self, dfIncludeId: pd.DataFrame) -> dict:
        uniqueCT = dfIncludeId['CreateTime'].unique()
        df_dict = {ct:dfIncludeId[dfIncludeId['CreateTime']==ct].reset_index(drop=True) for ct in uniqueCT}
        
        return df_dict
        

    def concatDataFrame(self, fs: gridfs.GridFS, dfIncludeId: dict) -> pd.DataFrame:
        uniqueCT = dfIncludeId['CreateTime'].unique()
        color_seq = ['R', 'G', 'B']
        df_list = []
        for ct in uniqueCT:
            df = dfIncludeId[dfIncludeId['CreateTime']==ct].reset_index(drop=True)
            for color in color_seq:
                color_df = df[df['LED_TYPE']==color].reset_index(drop=True)
                
                # 取得 Dataframe objectId 的位置 ex: color_df.iloc[0, 5]
                if fs == self.LUMfs:
                    color_df = self.getDataframeFromObjectID(fs=fs, objectID=color_df.iloc[0, 5])
                else:
                    color_df = self.getDataframeFromObjectID(fs=fs, objectID=color_df.iloc[0, 1])  
                df_list.append(color_df)
                
        return pd.concat(df_list).reset_index(drop=True)      
        
        
    def leave_specific_data(self, concated_df: pd.DataFrame, fileType: str) -> pd.DataFrame:
        """Filter dateframe by specific file's columns

        Args:
            df_type (str): 'LUM_EDC' or 'AOI_EDC'
        """
        
        filteredDataFrame = pd.DataFrame()
        
        # LUM 只取為暗點的 data
        if fileType == 'LUM_EDC':
            filteredDataFrame = concated_df[self.config['LUM_EDC_COL_NAME']]
            filteredDataFrame = filteredDataFrame[filteredDataFrame['Lighting_check']=='0']
            filteredDataFrame.drop(columns='Lighting_check', inplace=True) # 後面用不到
        
        # AOI 則取所有的 data    
        else:
            index_cols = [i for i in range(17)]
            filteredDataFrame = concated_df[index_cols]
            filteredDataFrame.columns = self.config['AOI_EDC_COL_NAME']
            leave_cols = ["CreateTime", "OPID", "LED_TYPE", "LED_Index_I", "LED_Index_J", "Defect Reciepe", "LINK"]
            filteredDataFrame = filteredDataFrame[leave_cols]
            # 將 defect code 換成對應值
            filteredDataFrame["Defect Reciepe"] = filteredDataFrame["Defect Reciepe"].apply(lambda x: self.defect_code_dict.get(x))
            filteredDataFrame['CreateTime'] = filteredDataFrame['CreateTime'].astype(str)
            
        return filteredDataFrame
        
    
    def opid_add_createTime(self, df: pd.DataFrame) -> pd.DataFrame:
        df['OPID'] = df['CreateTime'] + '_' + df['OPID']
        return df

    
    def get_preprocess_dataframe(self, fs: gridfs.GridFS, df: pd.DataFrame, fileType: str) -> pd.DataFrame:
        """Concatenate the RGB dataframes and unpack the object IDs from MongoDB using `GridFS`. 
        The data is concatenated by each color, and only specific columns are selected from the dataframe.

        Args:
            fs (gridfs.GridFS)
            df (pd.DataFrame): input dataframe
            fileType (str): LUM_EDC or AOI_EDC

        Returns:
            pd.DataFrame
        """
        final_df = self.concatDataFrame(fs=fs, dfIncludeId=df)
        final_df = self.leave_specific_data(concated_df=final_df, fileType=fileType)
        return final_df
    
    
    def link_Transfer_to_Map(self, df_aoi: pd.DataFrame) -> pd.DataFrame:
        for OPID in df_aoi['OPID'].unique():
            df_aoi[OPID + '_MAP'] = np.where(df_aoi['OPID']==OPID, df_aoi['LINK'], '')
        return df_aoi
        
    
    def adjust_col_value_by_file_type(self, pivoted_df: pd.DataFrame, fileType: str) -> pd.DataFrame:
        # 動態調整 column 的內的值 & map value to new column
        if fileType == 'LUM_EDC':
            # pivot 的 column name(OPID + CreateTime) 填入值
            for i in range(4, len(pivoted_df.columns.tolist())):
                pivoted_df.iloc[:, i] = np.where(pivoted_df.iloc[:, i]==1, 'LED 亮/暗(輝度)', '')
        else:
            # 將 Defect Reciepe 的值 map 到 pivot 的 column name(OPID + CreateTime)
            for i in range(6, len(pivoted_df.columns.tolist())):
                pivoted_df.iloc[:, i] = np.where(pivoted_df.iloc[:, i]==1, pivoted_df.iloc[:, 3], '')
            pivoted_df.drop(columns=['Defect Reciepe', 'LINK'], inplace=True) 
            
        return pivoted_df
        
        
    def pivotedDataframe(self, df: pd.DataFrame, fileType: str) -> pd.DataFrame:
        # for pivot
        df['CK'] = 1 
            
        cols = df.columns.to_list()
        cols.remove('CK')
        
        temp_pivot = pd.pivot_table(df, values='CK', index=cols, columns=['OPID']).reset_index()
        
        # 為了後面的 groupby 做準備，避免同一點位 & 顏色重複
        temp_pivot.drop(columns=['CreateTime', 'OPID'], inplace=True)
        cols.remove('CreateTime')
        cols.remove('OPID')
        temp_pivot = temp_pivot.groupby(by=cols).agg("sum").reset_index() 
        res_pivot_df = self.adjust_col_value_by_file_type(pivoted_df=temp_pivot, fileType=fileType)
               
        return res_pivot_df
    
    
    def addition_list_condiction(self, OPID: str|None=None, endswith_condiction: tuple|None=None, OPID_list: list|None=...) -> list:
        """Add OPID into specific OPID list by OPID endswith condiction
        
        Args:
            OPID (str): 
            endswith_condiction (tuple)
            OPID_list (list)

        Returns:
            list
            
        >>> Debond_list = []
        >>> OPID = "202311220021_MT+ACL"
        >>> ends_condiction = ('DE', '+ACL')
        >>> Debond_list = addition_list_condiction(OPID, ends_condiction, Debond_list)
        >>> Debond_list
        >>> ["202311220021_MT+ACL"]
        """
        
        if OPID_list is None:
            OPID_list = []
            
        if OPID.endswith(endswith_condiction):
            OPID_list.append(OPID)
        else:
            OPID_list.append('')
            
        return OPID_list
    
    
    
    def get_fixed_column(self, df: pd.DataFrame, stop_column_name: str) -> list:
        res_cols = df.columns.to_list()
        
        # reorder the col list
        fixed_col_limit = res_cols.index(stop_column_name)
        fixed_col = [res_cols[col] for col in range(fixed_col_limit + 1)]
        
        return fixed_col
        
        
    def merge_LUM_AOI_dataframe(self, df_lum: pd.DataFrame, df_aoi: pd.DataFrame) -> pd.DataFrame:
        df_aoi = self.get_preprocess_dataframe(fs=self.AOIfs, df=df_aoi, fileType='AOI_EDC')
        df_lum = self.get_preprocess_dataframe(fs=self.LUMfs, df=df_lum, fileType='LUM_EDC')
        
        df_aoi = self.opid_add_createTime(df_aoi)
        df_aoi = self.link_Transfer_to_Map(df_aoi)

        df_lum = self.opid_add_createTime(df_lum)
            
        temp_AOI_pivot = self.pivotedDataframe(df=df_aoi, fileType='AOI_EDC')
        temp_LUM_pivot = self.pivotedDataframe(df=df_lum, fileType='LUM_EDC')
        
        res = temp_LUM_pivot.merge(temp_AOI_pivot, how='outer', on=['LED_TYPE', 'LED_Index_I', 'LED_Index_J'])
        res.insert(loc=2, column='Inspection_Type', value=self.Inspection_Type)
        res = res.dropna(axis=0, how='any')
        
        fixed_col = self.get_fixed_column(df=res, stop_column_name='LED_Index_J')
        
        # 只選擇各站點一次(無重複站點)
        OPID_ls_sorted = []
        
        # for 選擇兩個以上重複的站點使用
        addition_lum_DE = []
        addition_lum_RE = []
        addition_aoi_DE = []
        addition_aoi_RE = []
        
        
        for opid_ends in self.opid_dict.keys():
            lum_opid_appear_cnt = 0
            for lum_opid in sorted(df_lum['OPID'].unique()):
                if lum_opid.endswith(opid_ends):
                    if lum_opid_appear_cnt < 1:
                        OPID_ls_sorted.append(lum_opid)
                        lum_opid_appear_cnt += 1
                    
                    else:
                        addition_lum_DE = self.addition_list_condiction(
                            OPID=lum_opid, endswith_condiction=('DE', '+ACL', '+ACL11', '+ACL21'), OPID_list=addition_lum_DE
                        )
                        
                        addition_lum_RE = self.addition_list_condiction(
                            OPID=lum_opid, endswith_condiction=('RE', '+ACL2', '+ACL12', '+ACL22'), OPID_list=addition_lum_RE
                        )

                        
            aoi_opid_appear_cnt = 0
            for aoi_opid in sorted(df_aoi['OPID'].unique()):
                if aoi_opid.endswith(opid_ends):
                    if aoi_opid_appear_cnt < 1:
                        OPID_ls_sorted.extend([aoi_opid, aoi_opid + '_MAP'])
                        aoi_opid_appear_cnt += 1
                        
                    else:
                        addition_aoi_DE = self.addition_list_condiction(
                            OPID=aoi_opid, endswith_condiction=('DE', '+ACO', '+ACO11', '+ACO21'), OPID_list=addition_aoi_DE
                        )

                        addition_aoi_RE = self.addition_list_condiction(
                            OPID=aoi_opid, endswith_condiction=('RE', '+ACO2', '+ACO12', '+ACO22'), OPID_list=addition_aoi_RE
                        )


        # reorder col 為了依站點排序 light on, AOI defect recipe, image link
        for lum_opid_DE, lum_opid_RE, aoi_opid_DE, aoi_opid_RE in zip(
            addition_lum_DE, addition_lum_RE, addition_aoi_DE, addition_aoi_RE
            ):
            
            if lum_opid_DE != '' and aoi_opid_DE != '':
                OPID_ls_sorted.append(lum_opid_DE)
                OPID_ls_sorted.append(aoi_opid_DE)
                OPID_ls_sorted.append(aoi_opid_DE + '_MAP')
                
            if lum_opid_RE != '' and aoi_opid_RE != '':
                OPID_ls_sorted.append(lum_opid_RE)
                OPID_ls_sorted.append(aoi_opid_RE)
                OPID_ls_sorted.append(aoi_opid_RE + '_MAP') 
        
        res = res[fixed_col + OPID_ls_sorted]
        res = res.groupby(fixed_col).agg("max").reset_index()
        # 最後一個AOI站點的原因
        res['Failure Analysis'] = res[(fixed_col + OPID_ls_sorted)[-2]]
        res['Solution'] = ''
        res['Short term Action'] = ''
        return res
    
    
        
