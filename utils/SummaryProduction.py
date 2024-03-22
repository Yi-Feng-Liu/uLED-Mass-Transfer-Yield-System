import pandas as pd
import numpy as np
from utils._config_ import *
import logging
from utils.sendMail import customMessageAutoMail
import json
from bson.binary import Binary
from pymongo import MongoClient, InsertOne
import pickle
import gridfs
import os
from bson import ObjectId
import math
import threading
import time
from sqlalchemy import create_engine
from utils._config_ import LogSaveDir
import psutil
import time


def resource_usage_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        # start_cpu_usage = psutil.cpu_percent(interval=None)
        start_ram_usage = psutil.virtual_memory().used

        result = func(*args, **kwargs)

        end_cpu_usage = psutil.cpu_percent(interval=None)
        end_ram_usage = psutil.virtual_memory().used
        end_time = time.time()

        # cpu_usage = end_cpu_usage - start_cpu_usage
        ram_usage = (end_ram_usage - start_ram_usage) / (1024**2) 

        print(f"{func.__name__} CPU usage: {end_cpu_usage:.2f}%")
        print(f"{func.__name__} RAM usage: {ram_usage:.2f} MB")
        print(f"{func.__name__} runtime: {end_time - start_time:.2f} 秒")
        return result
    return wrapper


class CreateLumSummary:
    """Write the dataframe into MonogDB"""
    
    def __init__(self):
        super().__init__()
        
        self.DBname = "MT"
        self.collection_name = "LUM_SummaryTable"
        self.client = MongoClient("mongodb://xxx:xxx@10.88.26.102:27017/?compressors=zlib")
        self.db = self.client[self.DBname]
        self.fs = gridfs.GridFS(self.db, self.collection_name)
        
        ### 取得不同檢測條件所對應的 dict of column name ###
        self.inspection_Lighting_Check_dict = column['Inspection_Lighting_check_dict']
        self.inspection_Defect_Code_dict = column['Inspection_Defect_Code_dict']
        self.inspection_Luminance_dict = column['Inspection_Luminance_dict']
        self.SHEET_ID = ""
        self.OPID = ""
        self.CHIP = ""
    
          
    @resource_usage_decorator      
    def drop_partA_info(self) -> pd.DataFrame:
        cols = self.assignCommonValue(self.origin_df)
        origin_df = self.origin_df.drop(index=self.origin_df.iloc[:len(cols)+1, 0].index.tolist())
        splited_df = self.expand_Dataframe(origin_df)
        splited_df = splited_df.reset_index(drop=True)
        
        return splited_df
    
    
    @resource_usage_decorator 
    def sort_coordinate_col(self) -> pd.DataFrame:
        self.splited_df[['LED_Index_I', 'LED_Index_J']] = self.splited_df[['LED_Index_I', 'LED_Index_J']].astype(int)
        self.splited_df = self.splited_df.reset_index(drop=True)
        
        return self.splited_df
    
        
    @resource_usage_decorator     
    def get_processed_splited_dataframe(self):
        self.splited_df = self.drop_partA_info()
        self.splited_df = self.fetch_columns()
        self.splited_df = self.sort_coordinate_col()
        
        return self.splited_df

    
    def haveChipValue(self, Series:pd.Series) -> bool:
        """Check the column `CHIP` if there is an exit chip value. And Skip Demura file

        Args:
            Series (pd.Series): column of chip

        Returns:
            bool
        """
        
        chipItem = Series.unique()
        if "" in chipItem or "AFTER" in chipItem or "BEFORE" in chipItem :
            return False
        return True
        
    
    def sort_and_reset_index(self, df:pd.DataFrame) -> pd.DataFrame:
        return df.sort_values(by=["LED_Index_I", "LED_Index_J"]).reset_index(drop=True)
        
    
    @resource_usage_decorator  
    def get_splited_and_RGB_dataframe(self) -> None:
        self.splited_df = self.get_processed_splited_dataframe()
        self.R_df = self.sort_and_reset_index(self.splited_df[self.splited_df["LED_TYPE"]=="R"])
        self.G_df = self.sort_and_reset_index(self.splited_df[self.splited_df["LED_TYPE"]=="G"])
        self.B_df = self.sort_and_reset_index(self.splited_df[self.splited_df["LED_TYPE"]=="B"])
        
        return self.splited_df, self.R_df, self.G_df, self.B_df


    @resource_usage_decorator 
    def assignCommonValue(self, df) -> list:
        """The common value means the dataframe of RGB they are have same columns

        Parameter:
        ----------
            df : the dataframe you input to the func

        Returns:
            columnlist: list
        """
        
        datalist = [i.split("=")[-1] for i in df["_FACTOR"].tolist()[0:13]]
        
        columnlist = []
        for i in datalist:
            if i != "_DATA":
                columnlist.append(i)
            else:
                break
                
        return columnlist    
    
    
    @resource_usage_decorator         
    def checkColumnList(self) -> tuple[str, ...]:
        """
        Returns:
            tuple[str, str, str, str, str, str]: TOOL_ID, MES_ID, SHEET_ID, MODEL_NO, ABBR_NO, OPID
        """
        
        columnlist = self.assignCommonValue(self.origin_df)
        
        TOOL_ID, MES_ID = columnlist[0:2]
        SHEET_ID, MODEL_NO,  ABBR_NO = columnlist[2:5]
        
        OPID, GRADE, STATUS = "", "", ""

        column_length = len(columnlist)
        if 5 <= column_length <= 7:
            _, OPID = columnlist[5:7]
        
        elif 7 <= column_length <= 9:
            _, OPID, GRADE, STATUS = columnlist[5:9]
            
            MODEL_NO = MODEL_NO if MODEL_NO else "V160SUN01-T1"
            OPID = OPID if OPID else "JI-DMU" 
                
        else:
            logging.error(f"Column Name {columnlist[7:]} cannot identify.")
        
        return TOOL_ID, MES_ID, SHEET_ID, MODEL_NO, ABBR_NO, OPID, GRADE, STATUS


    
    def get_return_value(self) -> tuple[str, ...]:
        """
        Returns:
            SHEET_ID, MODEL_NO, OPID
        """
        _, _, self.SHEET_ID, self.MODEL_NO, _, self.OPID, _, _ = self.checkColumnList()
        
        return self.SHEET_ID, self.MODEL_NO, self.OPID


    @resource_usage_decorator 
    def calculate_yield_column(
        self, 
        df: pd.DataFrame, 
        ok_column_name: str, 
        ng_column_name: str, 
        key: str
        ) -> pd.DataFrame:
        """Calculate the column of Yield.
        
        Groupby the dataframe then merge to original dataframe.
        
        Return:
            pd.Dataframe
        
        Examples
        --------
        >>> df = pd.DataFrame({
            "ID": ["A1", "A1", "A2", "A2"],
            "Defect": ["", "AB", "", "AB"],
            "NG": [0,1,0,3],
            "OK": [4,0,6,0],
        })
        >>> df.groupby(["ID"])[["NG", "OK"]].agg(sum).reset_index()
        >>> df
            SheetID  NG  OK
        0       A1    1   4
        1       A2    3   6
        """
        
        # 只取需要的column, 為了統一 defect code 的名稱
        key_list = ["CreateTime", "OPID", "ACTUAL_RECIPE", "LED_TYPE", self.inspection_Defect_Code_dict.get(key)]
        spec = ["CreateTime", "OPID", "ACTUAL_RECIPE", "LED_TYPE"]
        key_list2 = key_list + [ok_column_name, ng_column_name]
        
        df = df[key_list2]
        
        # 先做一次groupby 把各種defect分開
        df_group_split_defect = df.groupby(key_list)[[ok_column_name, ng_column_name]].agg("sum").reset_index()
        
        # 第二次 groupby 將 defect 的數量和 OK 數量進行整合 並計算良率
        df_group = df_group_split_defect.groupby(spec)[[ok_column_name, ng_column_name]].agg("sum").reset_index()
        del key_list, key_list2
        
        # edge_Dark_point 的良率是以四個邊的總和當分母
        if key == "edge_Dark_point":
            df_group["TOTAL"] = (self.width + self.height)*2
            df_group["OKCNT"] = df_group["TOTAL"] - df_group[ng_column_name]
            df_group["Yield"] = ((df_group["TOTAL"]-df_group[ng_column_name])/df_group["TOTAL"])*100
        else:
            df_group["TOTAL"] = df_group[ok_column_name] + df_group[ng_column_name]
            df_group["Yield"] = (df_group[ok_column_name] / df_group["TOTAL"])*100
            
        # 小數點後兩位無條件捨去
        try:        
            df_group["Yield"] = df_group["Yield"].apply(lambda x: math.floor(x*100)/100.0)
        except:
            df_group["Yield"] = ""
            
        # 避免重複的column 只取新增的
        key_list3 = spec + ["OKCNT", "TOTAL", "Yield"]
        df_group = df_group[key_list3]
        
        res_df = df_group_split_defect.merge(df_group, how="left", on=spec)
        del key_list3, spec, df_group, df_group_split_defect
        
        res_df.rename(
            columns = {
                self.inspection_Defect_Code_dict.get(key): "Defect_Code",
                "ABBR_No": "ABBR_NO",
            }, 
            inplace=True
        )
        
        return res_df
        
        
    @resource_usage_decorator 
    def CreateSummaryTable(
        self, 
        color_df: pd.DataFrame,
        key: str, 
        CHIP: str,
        haveChip: bool=False
        ) -> pd.DataFrame:
        """Create Summary Table using part A information
        
        param:
            color_df (pd.DataFrame): R, G and B dataframe, respctively.
            
        Returns:
            pd.DataFrame: Summary Table dataframe 
        """
            
        TOOL_ID, MES_ID, self.SHEET_ID, self.MODEL_NO, ABBR_NO, self.OPID, GRADE, STATUS = self.checkColumnList()
        
        self.height, self.width = self.ProductSizeFromModel(self.MODEL_NO)       
        if haveChip:
            self.width = int(self.width)
             
        if "BA0X" in color_df["Defect_Code"].unique():
            temp_df = color_df[color_df["Defect_Code"] != "BA0X"].reset_index(drop=True)
        
        else:
            temp_df = color_df[color_df["Defect_Code"] != "AB0X"].reset_index(drop=True)
        
        # 存入 DB 的 Dataframe
        for_db_df = temp_df.copy()
        for_db_df.drop(columns=["LED_Coordinate_X", "LED_Coordinate_Y"], inplace=True)
        
        try:
            temp_df["NGCNT"] = np.where((temp_df[self.inspection_Lighting_Check_dict.get(key)] == "0"), 1, 0)
            temp_df["OKCNT"] = np.where((temp_df[self.inspection_Lighting_Check_dict.get(key)] == "1"), 1, 0)
            if np.all(np.asarray(temp_df["OKCNT"], dtype=int)==0):
                temp_df["OKCNT"]=1 
        except:
            # if key is edge dark point
            temp_df["NGCNT"] = np.where((temp_df[self.inspection_Defect_Code_dict.get(key)] != ""), 1, 0)
            # 設 0 是因為上面的function 會將其 sum 起來
            temp_df["OKCNT"] = 0
            
        df_group = self.calculate_yield_column(df=temp_df, ok_column_name="OKCNT", ng_column_name="NGCNT", key=key)
        del temp_df
        
        # 創建一個字典，包含要插入的列
        new_columns = {
            "SHEET_ID": self.SHEET_ID, 
            "MES_ID": MES_ID, 
            "MODEL_NO": self.MODEL_NO, 
            "ABBR_NO": ABBR_NO, 
            "TOOL_ID": TOOL_ID,
            "Inspection_Type": key
        }

        # 使用assign方法批量添加新列
        df_group = df_group.assign(**new_columns)
        del new_columns
        
        # 指定列的順序
        column_order = [
            "SHEET_ID", 
            "MES_ID", 
            "MODEL_NO", 
            "ABBR_NO", 
            "TOOL_ID",
            "Inspection_Type",
            *df_group.columns[:-6]
        ]

        # 按照新的列順序重新索引
        df_group = df_group.reindex(columns=column_order)
        
        df_group.drop(columns="OKCNT_x", inplace=True)
        df_group.rename(columns={"OKCNT_y":"OKCNT"}, inplace=True)
        
        # 將原始資料寫入DB
        color_df_id = self.fs.put(Binary(pickle.dumps(for_db_df, protocol=5)))   
        del for_db_df
        
        col_list = [
            "CreateTime", "SHEET_ID", "MES_ID", "TOOL_ID", "MODEL_NO", "ABBR_NO", "ACTUAL_RECIPE", "OPID", "LED_TYPE", "Inspection_Type", "Defect_Code", "NGCNT", "OKCNT", "TOTAL", "Yield"
        ]
        
        df_group_tmp = df_group[col_list]
        del column_order, df_group, col_list
        
        df_group_tmp = df_group_tmp.copy()
        df_group_tmp["OPID"] = self.OPID
        df_group_tmp["CHIP"] = CHIP
        df_group_tmp["Grade"] = GRADE
        df_group_tmp["Status"] = STATUS
        df_group_tmp["Dataframe_id"] = color_df_id
        return df_group_tmp


    def convFilter(self, ksize: int, arr:np.ndarray, useWhere:bool):
        from scipy import signal
        kernel = np.ones((ksize, ksize), dtype=np.uint8)
        if useWhere:
            arr = np.where(arr >= 1, 1, 0)
        img = signal.convolve2d(arr, kernel, mode='valid')
        return img
    
    
    def createDataframeForInsertDB(
        self, 
        R_df: pd.DataFrame, 
        G_df: pd.DataFrame, 
        B_df: pd.DataFrame, 
        key: str,
        CHIP: str,
        haveChip: bool=False
    ) -> None: 
        
        ksize = 4
        ng_Four = ""
        ng_eight = ""
        ng_Five = ""
        byPixel_ng_Four = ""
        byPixel_ng_Five = ""
        byPixel_ng_eight = ""
        R_summary = self.CreateSummaryTable(R_df, key, CHIP=CHIP, haveChip=haveChip)
        G_summary = self.CreateSummaryTable(G_df, key, CHIP=CHIP, haveChip=haveChip)
        B_summary = self.CreateSummaryTable(B_df, key, CHIP=CHIP, haveChip=haveChip)
        
        R_dc_arr, G_dc_arr, B_dc_arr = self.defect_code_2D(key)
        R_lc_arr, G_lc_arr, B_lc_arr = self.LightingCheck_2D(key)
        R_lum_arr, G_lum_arr, B_lum_arr = self.Luminance_2D(key)
   
        Chromaticity_Rx_arr, Chromaticity_Gx_arr, Chromaticity_Bx_arr = self.Chromaticity_2D("CIE1931_Chromaticity_X")
        Chromaticity_Ry_arr, Chromaticity_Gy_arr, Chromaticity_By_arr = self.Chromaticity_2D("CIE1931_Chromaticity_Y")
        
        # 判斷回傳值是否為空字符串 如果是則不處理
        if isinstance(R_lc_arr, np.ndarray) and isinstance(R_lum_arr, np.ndarray):
            # 亮點轉暗點
            defect_arr_r = np.where(R_lc_arr == 1, 0, 1)
            defect_arr_g = np.where(G_lc_arr == 1, 0, 1)
            defect_arr_b = np.where(B_lc_arr == 1, 0, 1)
            
            white = defect_arr_r + defect_arr_g + defect_arr_b
            del defect_arr_r, defect_arr_g, defect_arr_b
            
            mainwhite = self.convFilter(ksize=ksize, arr=white, useWhere=False)
            byPixel_white = self.convFilter(ksize=ksize, arr=white, useWhere=True)
            ng_Four = np.count_nonzero(mainwhite >= 4)
            ng_eight = np.count_nonzero(mainwhite >= 8)
            ng_Five = np.count_nonzero(mainwhite >= 5)
            byPixel_ng_Four = np.count_nonzero(byPixel_white >= 4)
            byPixel_ng_eight = np.count_nonzero(byPixel_white >= 8)
            byPixel_ng_Five = np.count_nonzero(byPixel_white >= 5)
            del white, mainwhite, byPixel_white
            
            RLC_ID, RDC_ID, RLUM_ID, RChromaticity_x, RChromaticity_y = self.getObjectID(
                R_lc_arr, R_dc_arr, R_lum_arr, Chromaticity_Rx_arr, Chromaticity_Ry_arr
            )
            
            GLC_ID, GDC_ID, GLUM_ID, GChromaticity_x, GChromaticity_y = self.getObjectID(
                G_lc_arr, G_dc_arr, G_lum_arr, Chromaticity_Gx_arr, Chromaticity_Gy_arr
            )
            
            BLC_ID, BDC_ID, BLUM_ID, BChromaticity_x, BChromaticity_y = self.getObjectID(
                B_lc_arr, B_dc_arr, B_lum_arr, Chromaticity_Bx_arr, Chromaticity_By_arr
            )
            del R_lc_arr, G_lc_arr, B_lc_arr, R_dc_arr, G_dc_arr, B_dc_arr, R_lum_arr, G_lum_arr, B_lum_arr

            R_summary = self.assign_col(
                R_summary, LightingCheck_2D=RLC_ID, DefectCode_2D=RDC_ID, Luminance_2D=RLUM_ID, Chromaticity_X_2D=RChromaticity_x, Chromaticity_Y_2D=RChromaticity_y
            )
            
            G_summary = self.assign_col(
                G_summary, LightingCheck_2D=GLC_ID, DefectCode_2D=GDC_ID, Luminance_2D=GLUM_ID, Chromaticity_X_2D=GChromaticity_x, Chromaticity_Y_2D=GChromaticity_y
            )
            
            B_summary = self.assign_col(
                B_summary, LightingCheck_2D=BLC_ID, DefectCode_2D=BDC_ID, Luminance_2D=BLUM_ID, Chromaticity_X_2D=BChromaticity_x, Chromaticity_Y_2D=BChromaticity_y
            )
            
        whole_df = pd.concat([R_summary, G_summary, B_summary])
        del R_summary, G_summary, B_summary
        
        whole_df['Kernel'] = ksize
        whole_df['4EA'] = ng_Four
        whole_df['5EA'] = ng_Five
        whole_df['8EA'] = ng_eight
        whole_df['P_4EA'] = byPixel_ng_Four
        whole_df['P_5EA'] = byPixel_ng_Five
        whole_df['P_8EA'] = byPixel_ng_eight
        self.insert_dataframe_to_mongoDB(whole_df=whole_df, collection_name="LUM_SummaryTable")
        del whole_df
        
        
    ### Deprecated
    # def specific_area_arr(self, arr:np.ndarray, coc2x:int, coc2y:int) -> np.ndarray:
    #     """Light check series trans to  coc2 correspond area, and append to new array (4-D).
        
    #     example 
        
    #     First Step: (480, 270) transpose to (270, 480)
        
    #     Second Step: get each size area from `MODEL_NO` by product.
    #     """
    #     arr = arr.T 
    #     area_np_arr = np.zeros((coc2x ,coc2y), dtype=int)
    #     return area_np_arr
        

    # def get_COC2_arr_size(self) -> np.ndarray:
    #     COC2_X, COC2_Y = self.ProductSizeFromModel(self.MODEL_NO)
    #     coc2_arr = np.zeros((COC2_X ,COC2_Y), dtype=int)
    #     return coc2_arr

    @resource_usage_decorator 
    def concatRGBdf(self, R_BOND_DF, G_BOND_DF, B_BOND_DF) -> pd.DataFrame:
        return pd.concat([R_BOND_DF, G_BOND_DF, B_BOND_DF])


    @resource_usage_decorator 
    def getObjectID(self, LC_2D:np.ndarray, defect_code_2D:np.ndarray, LUM2D:np.ndarray, 
                    CIEX_2D:np.ndarray,CIEY_2D:np.ndarray) -> tuple[ObjectId, ObjectId, ObjectId, ObjectId, ObjectId]:
        """Return the ObjectID"""
        
        # 轉置成 270, 480 後, 先上下再左右翻轉 以符合 light on 檢結果
        try:
            LC_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LC_2D.T, 0), 1), protocol=5)))
            DC_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(defect_code_2D.T, 0), 1), protocol=5)))
            LUM_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LUM2D.T, 0), 1), protocol=5)))
        except:
            LC_id, DC_id, LUM_id = "", "", ""
            
        CIEX_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(CIEX_2D.T, 0), 1), protocol=5)))
        CIEY_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(CIEY_2D.T, 0), 1), protocol=5)))
        
        return LC_id, DC_id, LUM_id, CIEX_id, CIEY_id


    ### Deprecated   
    # def reshape_4d_arr_to_2d(self, coc2_arr, coc2_x) -> np.ndarray:
    #     init_tuple = ()
    #     for y in range(coc2_x):
    #         e = np.concatenate(coc2_arr[y][:], axis=1)
    #         init_tuple += tuple(e)
    #     res_2d = np.vstack(init_tuple)
    #     return res_2d
    
    
    @resource_usage_decorator 
    def Chromaticity_2D(self, Chromaticity:str) -> np.ndarray:
        """Return R & G & B Chromaticity 2D array (dtype is string).

        Parameters:
            Chromaticity (str): Chromaticity_X or Chromaticity_Y

        Returns:
            array
        """

        try:
            Chromaticity_R_arr = np.asarray(self.R_df[Chromaticity][:], dtype=str).reshape(self.height, self.width)
            Chromaticity_G_arr = np.asarray(self.G_df[Chromaticity][:], dtype=str).reshape(self.height, self.width)
            Chromaticity_B_arr = np.asarray(self.B_df[Chromaticity][:], dtype=str).reshape(self.height, self.width)
        except:
            Chromaticity_R_arr, Chromaticity_G_arr, Chromaticity_B_arr = "", "", ""
            
        return Chromaticity_R_arr, Chromaticity_G_arr, Chromaticity_B_arr 
    
    
    @resource_usage_decorator 
    def LightingCheck_2D(self, key:str) -> np.ndarray:
        """Return lighting ckeck array by specific inspection type.

        Parameters:
            key (str): Inspection type

        Returns:
            array
        """

        try:
            RlightingArray = np.asarray(self.R_df[self.inspection_Lighting_Check_dict.get(key)][:], dtype=int).reshape(self.height, self.width)
            GlightingArray = np.asarray(self.G_df[self.inspection_Lighting_Check_dict.get(key)][:], dtype=int).reshape(self.height, self.width)
            BlightingArray = np.asarray(self.B_df[self.inspection_Lighting_Check_dict.get(key)][:], dtype=int).reshape(self.height, self.width)
        except:
            RlightingArray, GlightingArray, BlightingArray = "", "", "" 
            
        return RlightingArray, GlightingArray, BlightingArray   
    

    @resource_usage_decorator 
    def Luminance_2D(self, key:str) -> np.ndarray:
        """Return Luminance_2D array id by specific inspection type.

        Parameters:
            key (str): Inspection type
        """
        try:
            R_lum_2d = np.asarray(self.R_df[self.inspection_Luminance_dict.get(key)][:], dtype=float).reshape(self.height, self.width)
            G_lum_2d = np.asarray(self.G_df[self.inspection_Luminance_dict.get(key)][:], dtype=float).reshape(self.height, self.width)
            B_lum_2d = np.asarray(self.B_df[self.inspection_Luminance_dict.get(key)][:], dtype=float).reshape(self.height, self.width)
        except:
            R_lum_2d, G_lum_2d, B_lum_2d = "", "", "" 
            
        return R_lum_2d, G_lum_2d, B_lum_2d


    @resource_usage_decorator 
    def defect_code_2D(self, key:str) -> np.ndarray:
        """Return defect code 2D array by specific inspection type.

        Parameter:
        -----------
            key (str): Inspection type
        """

        try:
            R_DC = np.asarray(self.R_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str).reshape(self.height, self.width)
            G_DC = np.asarray(self.G_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str).reshape(self.height, self.width)
            B_DC = np.asarray(self.B_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str).reshape(self.height, self.width)
        except:
            R_DC, G_DC, B_DC = "", "", ""
            
        return R_DC, G_DC, B_DC
        

    @resource_usage_decorator 
    def assign_col(self, df_group_tmp, **kwargs) -> pd.DataFrame:
        """Assign value to Summary Table dataframe and specific column
        
        Returns:
            pd.DataFrame: assigned dataframe 
        """
        
        summary_df = df_group_tmp.copy()
        summary_df = summary_df.assign(
            LightingCheck_2D = kwargs.get("LightingCheck_2D", ""),
            DefectCode_2D = kwargs.get("DefectCode_2D", ""),
            Luminance_2D = kwargs.get("Luminance_2D", ""),
            Chromaticity_X_2D = kwargs.get("Chromaticity_X_2D", ""),
            Chromaticity_Y_2D= kwargs.get("Chromaticity_Y_2D", ""),
        )
        return summary_df
    
    
    @resource_usage_decorator 
    def insert_dataframe_to_mongoDB(self, whole_df:pd.DataFrame, collection_name:str) -> None:
        """Insert the dataframe to MongoDB

        Parameter:
        ----------
            whole_df (pd.DataFrame): the dataframe you want to insert to mongoDB
        """
        # connect to MongoDB
        db = self.client[self.DBname]
        collection = db[collection_name] 
        
        # insert to Mongo DB
        if collection not in self.client.list_database_names():
            collection.create_index([("CreateTime", 1), ("SHEET_ID", 1), ("OPID", 1), ("LED_TYPE", 1), ("Defect_Code", 1)])
            result = whole_df.to_json(orient="records", default_handler=str)
            
        else:
            result = whole_df.to_json(orient="values", default_handler=str)
        
        parsed = json.loads(result)
        operation = [InsertOne(doc) for doc in parsed]
        collection.bulk_write(operation)


    def creatLogFile(self, filename):
        path = f"{LogSaveDir}{filename}"
        
        if not os.path.exists(LogSaveDir):
            os.mkdir(LogSaveDir)
            
        logging.basicConfig(
            filename = path, 
            filemode = "a", 
            format = "%(asctime)s - %(message)s", 
            encoding = "utf-8"
        )
        
    def ProductSizeFromModel(self, MODEL_NO: str) -> tuple[int, ...]:
        """
        Query the database to retrieve the product size corresponding from the given `MODEL_NO`.

        Params
        ------
        MODEL_NO : str
        
        Return
        -------
        x (int): Size of Panel x
        y (int): Size of Panel y
        """
        
        engine = create_engine("oracle://L4A_ULEDMFG_AP:L4A_ULEDMFG$AP@LTH41-VIP:1521/?service_name=L4AULEDH")
        conn1 = engine.connect()

        sql = f"""
            select A.MODEL_NO, A.signal_no as X, A.gate_no as Y
            from CELODS.C_ROU_PROD_MST A
            where A.model_no = '{MODEL_NO}' and A.signal_no is not null and A.gate_no is not null 
            group by A.MODEL_NO, A.signal_no, A.gate_no
            """
            
        df = pd.read_sql(sql, con=conn1)
        conn1.close()
        
        return df['x'].max(), df['y'].max()
    
    
    @resource_usage_decorator 
    def processRowData(self, file:str) -> pd.DataFrame:
        """Process the row csv and rename the all columns

        Parameter:
        ----------
            file_path (str): io

        Returns:
            pd.DataFrame: _description_
        """
        if file != None:
            try:
                self.origin_df = pd.read_csv(file, sep="dialect", engine="python", encoding="utf-8-sig")
            except:
                self.origin_df = pd.read_csv(file, sep="dialect", engine="python", encoding="latin-1")
    
            return self.origin_df
    
    
    @resource_usage_decorator 
    def expand_Dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand part A of dataframe using comma"""
        try:
            splited_df = df["_FACTOR"].str.split(",", expand=True)
        except:
            splited_df = pd.DataFrame()
            
        return splited_df
    
    
    # 將不足目標column數的dataframe補齊columns數，並更改column name。
    @resource_usage_decorator 
    def fetch_columns(self):
        if len(self.splited_df.columns) < len(self.column_list):
            gap = len(self.column_list) - len(self.splited_df.columns)
            cols_dict = {f"Unamed{i}": "" for i in range(gap)}
            self.splited_df = self.splited_df.assign(**cols_dict)
        self.splited_df.columns = self.column_list
        
        return self.splited_df
    

class csv_pre_filter(CreateLumSummary):
    """Pre filter csv file by MODEL_NO and its corresponding product size"""
    def __init__(self) -> None:
        super().__init__()
        self.column_list = column['LUM_EDC']
        self.MODEL_TYPE_DIC = MODEL_TYPES
        self.splited_df = pd.DataFrame()
        self.origin_df = pd.DataFrame()
        self.MODEL_NO = ""
        self.creatLogFile("running.log")
        self.height = int
        self.width = int
        
        # ### 取得不同檢測條件的 list ###
        self.inspection_type_list = setupJSON_load['Inspection_Types']

        
    def __call__(self, file_list):
        self.file_list = sorted(file_list, key=lambda t: os.stat(t).st_mtime)
        self.check_model_no()
        self.height, self.width = self.ProductSizeFromModel(self.MODEL_NO)
        
        # return filter_data_list
    
    
    @resource_usage_decorator 
    def RGB_df_FromRowData(self) -> None:
        self.R_df = self.splited_df[self.splited_df["LED_TYPE"]=="R"].reset_index(drop=True)
        self.G_df = self.splited_df[self.splited_df["LED_TYPE"]=="G"].reset_index(drop=True)
        self.B_df = self.splited_df[self.splited_df["LED_TYPE"]=="B"].reset_index(drop=True)


    @resource_usage_decorator    
    def dataActualSize(self) -> tuple[int, ...]:
        if len(self.R_df.index) != 0:
            data_df = self.R_df
            
        elif len(self.G_df.index) != 0:
            data_df = self.G_df
            
        elif len(self.B_df.index) != 0:
            data_df = self.B_df
            
        else:
            return 0, 0
        
        height = max(data_df["LED_Index_I"].astype(int))
        width = max(data_df["LED_Index_J"].astype(int))
            
        return height, width


    @resource_usage_decorator 
    def responseErrorMessage(self, data_height: int, data_width: int) -> None:
        """Verify the MODEL NO and retrieve its actual size from the csv file.

        Args:
            data_height (int): Max value of dataframe LED_Index_I
            data_width (int): Max value of dataframe LED_Index_J
        """
        subject = f"MODEL_NO {self.MODEL_NO} Warning"
        
        mail_message = (
            f"[Warning] MODEL_NO = {self.MODEL_NO}<br>\
            [Warning] {self.file}<br>\
            [Warning] Height = {data_height} & Width = {data_width}  "
        )
        
        def change_message_type(message:str, HTML_fomat:bool):
            if HTML_fomat:
                return message.replace("\n", "<br>")
            return message.replace("<br>", "\n")
            
        logging.warning(change_message_type(mail_message, HTML_fomat=False))

        send_message = customMessageAutoMail()
        thread1 = threading.Thread(target=send_message, args=(subject, mail_message))
        thread1.start()
        thread1.join()
        
    
    def fileNameRuleIsQualified(self, lst) -> bool:
        return any(lst)
    
    
    @resource_usage_decorator 
    def checkProductAndFileName(self, actual_sheet_id: str) -> bool:
        # bool dictionary
        check = {
            '161': "vkv" in actual_sheet_id or 'yu' in actual_sheet_id,
            '173': "vxt" in actual_sheet_id or "vxs" in actual_sheet_id,
            'Z300': "vbv" in actual_sheet_id or "vbt" in actual_sheet_id,
            '136': len(actual_sheet_id)==6,
            '183': 'hnz' in actual_sheet_id,
        }
        bool_list = [value for _, value in check.items()]
        qualifiedFile = self.fileNameRuleIsQualified(bool_list)
        return qualifiedFile
    
    
    def AOI_main_procedure(self, key: str) -> None:

        SHEET_ID, _, _ = self.get_return_value()
        if SHEET_ID != "":
            logging.warning(f"[INFO] Running {SHEET_ID}, Inspection Type {key}")

            processed_full_df, R_df, G_df, B_df = self.get_splited_and_RGB_dataframe()
            haveChip = self.haveChipValue(processed_full_df['CHIP']) # 第24個欄位是CHIP欄位
            
            if haveChip:
                chipItem = processed_full_df['CHIP'].unique()
                for chip in chipItem:
                    R_df = R_df[R_df['CHIP']==chip]
                    G_df = G_df[G_df['CHIP']==chip]
                    B_df = B_df[B_df['CHIP']==chip]
            else:
                chip = ""
                
            self.createDataframeForInsertDB(R_df, G_df, B_df, key, chip, haveChip)
            
            
    @resource_usage_decorator 
    def check_model_no(self) -> list:
        """Check file"s MODEL_NO and there width and height.

        Returns:
            list: the file list pass the condiction 
        """
        
        print("[INFO] Checking File's MODEL_NO and Size...")
        
        # checked_file_list = []
        for self.file in self.file_list:
            actual_sheet_id = self.file.split("_")[1].lower()
            qualifiedFile = self.checkProductAndFileName(actual_sheet_id)
            if "test" in self.file.lower(): 
                logging.warning(f"[Warning] Skipped file name {self.file.split('_')[1]}")
                continue

            elif qualifiedFile:
                logging.warning(f"[INFO] Checking {self.file}")

                start_t = time.time()
                self.origin_df = self.processRowData(self.file)
                self.splited_df = self.expand_Dataframe(self.origin_df)
                
                if len(self.splited_df.index) != 0:
                    self.splited_df = self.fetch_columns()
                    
                    logging.warning(f"[INFO] Process File Cost {(time.time() - start_t):.2f} secs")
                    del start_t
                    
                    self.RGB_df_FromRowData()
                    data_height, data_width = self.dataActualSize()
                    self.MODEL_NO = self.splited_df.iloc[3,0].split("=")[-1] # get MODEL_NO
                    
                    if self.MODEL_NO != "":
                        standard_h, standard_w = self.ProductSizeFromModel(self.MODEL_NO)
                        
                        if "Z300" in self.MODEL_NO:
                            standard_w = int(standard_w / 2)
                            
                        # 不符合規格的資料 寄Email提醒
                        if data_height != standard_h or data_width != standard_w:
                            self.responseErrorMessage(
                                data_height = data_height,
                                data_width = data_width,
                            )
                        
                        else:
                            logging.warning(f"[INFO] Pass")
                            for key in self.inspection_type_list:
                                try:
                                    self.AOI_main_procedure(key)
                                except Exception as e:
                                    logging.warning(f'[NOTICE] {self.file} has been skipped due to an error: ')
                                    logging.warning(f'[ERROR] {str(e)}')
                                    continue 

                            
                    else:
                        self.MODEL_NO = "V160SUN01-T1"
                        for key in self.inspection_type_list:
                            self.AOI_main_procedure(key)
                        # logging.warning(f"[Warning] MODEL NO is Empty")
                        # continue
                else:
                    logging.warning(f"[Error] Unexpected error occurred while expanding DataFrame")
                    continue
            else: 
                logging.warning(f"[Warning] Skip File name {self.file.split('_')[1]}")
                continue

        

        

        
