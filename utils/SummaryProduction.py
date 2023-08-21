import pandas as pd
import numpy as np
from utils._config_ import T161FUN01, T136FUN01, T173XUN01, V130FLN02
import numpy.typing as npt
import logging
from utils._BondingSummaryProduction_ import bonding_processing
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

class Summary_produce(bonding_processing):
    """Write the dataframe into MonogDB"""
    
    def __init__(self):
        super().__init__()
        self.DBname = 'TEST'
        self.collection_name = 'LUM_SummaryTable'
        self.client = MongoClient('mongodb://user:account@10.88.26.102:27017/?compressors=zlib')
        self.db = self.client[self.DBname]
        self.fs = gridfs.GridFS(self.db, self.collection_name)
        self.inspection_type_list = ['L255', 'L10', 'L0', 'edge_Dark_point']
        
        self.inspection_Lighting_check_dict = {
            'L255': 'Lighting_check',
            'L10': 'L10_Lighting_check',
            'L0': 'L0_Lighting_check',
            'edge_Dark_point': '',
        }
        
        self.inspection_Defect_Code_dict = {
            'L255': 'Defect_Code',
            'L10': 'Defect_Code_L10',
            'L0': 'Defect_Code_L0',
            'edge_Dark_point': 'edge_Dark_point',
        }
        
        self.inspection_Luminance_dict = {
            'L255': 'LED_Luminance',
            'L10': 'L10_LED_Luminance',
            'L0': 'L0_LED_Luminance',
            'edge_Dark_point': '',
        }
        
        self.column_list = [
            'CreateTime', 'OPID', 'EQP_ID', 'MODEL_NO', 'ABBR_No', 'ACTUAL_RECIPE', 'LED_TYPE',
            'LED_Coordinate_X', 'LED_Index_I', 'LED_Coordinate_Y', 'LED_Index_J', 
            'Lighting_check', 'Defect_Code', 'LED_Luminance',
            'CIE1931_Chromaticity_X', 'CIE1931_Chromaticity_Y', 'Target_Carrier_ID',
            'L0_Lighting_check', 'Defect_Code_L0', 'L0_LED_Luminance',
            'L10_Lighting_check', 'Defect_Code_L10', 'L10_LED_Luminance', 'edge_Dark_point'
        ] 
        
        # product 13.6 and product 13.0 are the same size (480, 270)
        self.MODEL_TYPE_DIC = {
            '13.6': ['T136FUN01.0', 'Y136FLN03.0', 'V130FLN02', 'Y136FLN03-T0', 'Y136FLN03-T1'],
            '16.1': ['T161XUN01.0', 'T161FUN01.0', 'V160SUN01-T0', 'V160SUN01-T1'],
            '17.3': ['T173XUN01.1', 'Y173EUN01', 'Y173EUN01-T1', 'Y173EUN01-T0']
        }
        
        self.MB_history_types = ['TNLBO', 'UM-BON', 'MT-ACL', 'MT - ACL']
        self.MODEL_NO = ''
        self.SHEET_ID = ''
        self.OPID = ''
        self.w = 0
        self.h = 0


    def get_h_w_info(self):
        """Return product height and width from MODEL_NO"""
        h, w = 0, 0
        
        if '13' in self.MODEL_NO:
            h = T136FUN01["COC2_Y_PIXEL"]*T136FUN01["SW_Y_COC2"]
            w = T136FUN01["COC2_X_PIXEL"]*T136FUN01["SW_X_COC2"]
        elif '16' in self.MODEL_NO:
            h = T161FUN01["COC2_X_PIXEL"]*T161FUN01["SW_X_COC2"]
            w = T161FUN01["COC2_Y_PIXEL"]*T161FUN01["SW_Y_COC2"]
        elif '17' in self.MODEL_NO:
            h = T173XUN01["COC2_Y_PIXEL"]*T173XUN01["SW_Y_COC2"]
            w = T173XUN01["COC2_X_PIXEL"]*T173XUN01["SW_X_COC2"]
        return h, w
    
            
    def process_row_data(self, file:str) -> None:
        """Process the row csv and rename the all columns

        Parameter:
        ----------
            file_path (str): io

        Returns:
            pd.DataFrame: _description_
        """
        try:
            self.df = pd.read_csv(file, sep='dialect', engine='python', encoding="utf-8-sig")
        except:
            self.df = pd.read_csv(file, sep='dialect', engine='python', encoding="latin-1")
        self.new_df = self.df['_FACTOR'].str.split(',', expand=True)
     

    def rename_new_df_and_turn_in_column(self) -> None:
        self.new_df.drop(index=self.new_df.iloc[:8, 0].index.tolist(), inplace=True)
        self.new_df = self.new_df.reset_index(drop=True)
        # 判斷dataframe長度是否跟預設的columns數量相同
        if len(self.new_df.columns) < len(self.column_list):
            gap = len(self.column_list) - len(self.new_df.columns)
            for i in range(gap):
                self.new_df[f'Unamed{i}'] = ''
        self.new_df.columns = self.column_list
        self.new_df['LED_Index_I'] = self.new_df['LED_Index_I'].astype(int)
        self.new_df['LED_Index_J'] = self.new_df['LED_Index_J'].astype(int)
       


    def RGB_df_FromRowData(self) -> None:
        self.rename_new_df_and_turn_in_column()
        self.R_df = self.new_df[self.new_df['LED_TYPE']=='R']
        self.R_df = self.R_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
        self.G_df = self.new_df[self.new_df['LED_TYPE']=='G']
        self.G_df = self.G_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
        self.B_df = self.new_df[self.new_df['LED_TYPE']=='B']
        self.B_df = self.B_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
    
    
    def get_return_dataframe(self) -> pd.DataFrame:
        """return Dataframe need to be pass in function.

        Returns:
            self.new_df, self.R_df, self.G_df, self.B_df: pd.DataFrame
        """
        self.RGB_df_FromRowData()
        return self.new_df, self.R_df, self.G_df, self.B_df
    
        
    def assignCommonValue(self) -> list:
        """The common value means the dataframe of RGB they are have same columns

        Parameter:
        ----------
            df : the dataframe you input to the func

        Returns:
            columnlist: list
        """
        
        columnlist = []
        init_len = [i.split('=')[-1] for i in self.df['_FACTOR'].tolist()[0:5]]
        datalist = [i.split('=')[-1] for i in self.df['_FACTOR'].tolist()[0:13]]

        for i in datalist:
            if i not in init_len and i != '_DATA':
                logging.info(f"[INFO] {i} was append to New column") 
            if i != '_DATA':
                columnlist.append(i)
            else:
                break
        return columnlist    
    
            
    def checkColumnList(self) -> str:
        columnlist = self.assignCommonValue()
        TOOL_ID, MES_ID, SHEET_ID, MODEL_NO,  ABBR_NO, ACTUAL_RECIPE, OPID = 0, 0, 0, 0, 0, 0, 0
        if len(columnlist) <= 5:
            TOOL_ID, MES_ID = columnlist[0:2]
            SHEET_ID, MODEL_NO,  ABBR_NO = columnlist[2:5]
            ACTUAL_RECIPE = 0
            OPID = 0
        elif 7 >= len(columnlist) >= 5:
            TOOL_ID, MES_ID = columnlist[0:2]
            SHEET_ID, MODEL_NO,  ABBR_NO = columnlist[2:5]
            ACTUAL_RECIPE, OPID = columnlist[5:7]
        else:
            logging.error(f"Column Name {columnlist[7:]} cannot identify.")
            message = f'Column Name {columnlist[7:]} cannot identify.'
            customMessageAutoMail().send(message)
        del columnlist
        return TOOL_ID, MES_ID, SHEET_ID, MODEL_NO, ABBR_NO, OPID 


    def coc2_area_dict(self, types:str) -> dict:
        """Repair coc2 area in UMBON200"""
        
        if types=='13.6':
            dict136 = {
                'A':[0,0],
                'B':[0,1],
                'C':[0,2],
                'D':[1,0],
                'E':[1,1],
                'F':[1,2]
            }
            return dict136

        elif types=='16.1':
            dict161 = {
                'A':[0,0],
                'B':[0,1],
                'C':[0,2],
                'D':[0,3],
                'E':[1,0],
                'F':[1,1],
                'G':[1,2],
                'H':[1,3]
            }
            return dict161
        
        elif types=='17.3':
            dict173 = {
                'A':[0,0],
                'B':[0,1],
                'C':[0,2],
                'D':[0,3],
                'E':[1,0],
                'F':[1,1],
                'G':[1,2],
                'H':[1,3],
                'I':[2,0],
                'J':[2,1],
                'K':[2,2],
                'L':[2,3]
            }
            return dict173


    def reshape_following_data(self) -> int:
        if len(self.R_df.index) != 0:
            self.height, self.width = self.R_df['LED_Index_I'].max(), self.R_df['LED_Index_J'].max()
        elif len(self.G_df.index) != 0:
            self.height, self.width = self.G_df['LED_Index_I'].max(), self.G_df['LED_Index_J'].max()
        elif len(self.B_df.index) != 0:
            self.height, self.width = self.B_df['LED_Index_I'].max(), self.B_df['LED_Index_J'].max() 
        return self.height, self.width


    def get_return_value(self) -> str:
        """
        Returns:
            SHEET_ID, MODEL_NO, OPID: str
        """
        _, _, self.SHEET_ID, self.MODEL_NO, _, self.OPID = self.checkColumnList()
        return self.SHEET_ID, self.MODEL_NO, self.OPID

    
    def calculate_yield_column(self, df:pd.DataFrame, ok_column_name:str, ng_column_name:str, key) -> pd.DataFrame:
        """Calculate the column of Yield.
        
        Groupby the dataframe then merge to original dataframe.
        
        Return:
            pd.Dataframe
        
        Examples
        --------
        >>> df = pd.DataFrame({
            'ID': ['A1', 'A1', 'A2', 'A2'],
            'Defect': ['', 'AB', '', 'AB'],
            'NG': [0,1,0,3],
            'OK': [4,0,6,0],
        })
        >>> df.groupby(['ID'])[['NG', 'OK']].agg(sum).reset_index()
        >>> df
            SheetID  NG  OK
        0       A1    1   4
        1       A2    3   6
        """
        
        # 只取需要的column, 為了統一 defect code 的名稱
        key_list = ['CreateTime', 'OPID', 'ACTUAL_RECIPE', 'LED_TYPE', self.inspection_Defect_Code_dict.get(key)]
        
        spec = ['CreateTime', 'OPID', 'ACTUAL_RECIPE', 'LED_TYPE']
        
        key_list2 = key_list + [ok_column_name, ng_column_name]
        
        df = df[key_list2]
        
        # 先做一次groupby 把各種defect分開
        df_group_split_defect = df.groupby(key_list)[[ok_column_name, ng_column_name]].agg('sum').reset_index()
        
        # 第二次 groupby 將 defect 的數量和 OK 數量進行整合 並計算良率
        df_group = df_group_split_defect.groupby(spec)[[ok_column_name, ng_column_name]].agg('sum').reset_index()
        
        # edge_Dark_point 的良率是以四個邊的總和當分母
        if key == 'edge_Dark_point':
            self.h, self.w = self.get_h_w_info()
            df_group['TOTAL'] = (self.w + self.h)*2
            df_group['OKCNT'] = df_group['TOTAL'] - df_group[ng_column_name]
            df_group['Yield'] = ((df_group['TOTAL']-df_group[ng_column_name])/df_group['TOTAL'])*100
        else:
            df_group['TOTAL'] = df_group[ok_column_name] + df_group[ng_column_name]
            df_group['Yield'] = (df_group[ok_column_name] / df_group['TOTAL'])*100
            
        # 小數點後兩位無條件捨去
        try:        
            df_group['Yield'] = df_group['Yield'].apply(lambda x: math.floor(x*100)/100.0)
        except:
            df_group['Yield'] = ''
            
        # 避免重複的column 只取新增的
        key_list3 = spec + ['OKCNT', 'TOTAL', 'Yield']
        df_group = df_group[key_list3]
        res_df = df_group_split_defect.merge(df_group, how='left', on=spec)
        res_df.rename(columns={
            self.inspection_Defect_Code_dict.get(key): 'Defect_Code',
            'ABBR_No': 'ABBR_NO',
        }, inplace=True)
        return res_df
        

    def CreateSummaryTable(self, color_df:pd.DataFrame, key:str, Grade:str) -> pd.DataFrame:
        """Create Summary Table using part A information
        
        param:
            color_df (pd.DataFrame): R, G and B dataframe, respctively.
            
        Returns:
            pd.DataFrame: Summary Table dataframe 
        """
        TOOL_ID, MES_ID, self.SHEET_ID, self.MODEL_NO, ABBR_NO, self.OPID = self.checkColumnList()
        
        new_df_tmp = color_df.copy()
        new_df_tmp = new_df_tmp[new_df_tmp['Defect_Code'] != 'BA0X'].reset_index(drop=True)
        
        # 存入 DB 的 Dataframe
        for_db_df = new_df_tmp.copy()
        for_db_df.drop(columns=['LED_Coordinate_X', 'LED_Coordinate_Y'], inplace=True)
        
        try:
            new_df_tmp['NGCNT'] = np.where((new_df_tmp[self.inspection_Lighting_check_dict.get(key)] == '0'), 1, 0)
            new_df_tmp['OKCNT'] = np.where((new_df_tmp[self.inspection_Lighting_check_dict.get(key)] == '1'), 1, 0)
            if np.all(np.asarray(new_df_tmp['OKCNT'], dtype=int)==0):
                new_df_tmp['OKCNT']=1 
        except:
            # if key is edge dark point
            new_df_tmp['NGCNT'] = np.where((new_df_tmp[self.inspection_Defect_Code_dict.get(key)] != ''), 1, 0)
            # 設 0 是因為上面的function 會將其 sum 起來
            new_df_tmp['OKCNT'] = 0
        
        df_group = self.calculate_yield_column(df=new_df_tmp, ok_column_name='OKCNT', ng_column_name='NGCNT', key=key)
        df_group.insert(loc=1, column='SHEET_ID', value=self.SHEET_ID)
        df_group.insert(loc=4, column='MES_ID', value=MES_ID)
        df_group.insert(loc=5, column='MODEL_NO', value=self.MODEL_NO)
        df_group.insert(loc=6, column='ABBR_NO', value=ABBR_NO)
        df_group.insert(loc=7, column='TOOL_ID', value=TOOL_ID)
        df_group.insert(loc=8, column='Inspection_Type', value=key)
        
        df_group.drop(columns='OKCNT_x', inplace=True)
        df_group.rename(columns={'OKCNT_y':'OKCNT'}, inplace=True)
        
        color_df_id = self.fs.put(Binary(pickle.dumps(for_db_df, protocol=5)))   
        
        key_list = ['CreateTime', 'SHEET_ID', 'MES_ID', 'TOOL_ID', 'MODEL_NO', 'ABBR_NO', 'ACTUAL_RECIPE', 'OPID', 'LED_TYPE', 'Inspection_Type', 'Defect_Code', 'NGCNT', 'OKCNT', 'TOTAL', 'Yield']
        
        df_group_tmp = df_group[key_list]
        df_group_tmp = df_group_tmp.copy()
        
        df_group_tmp['Grade'] = Grade
        df_group_tmp['Dataframe_id'] = color_df_id
        
        return df_group_tmp

    
    def specific_area_arr(self, arr:npt.ArrayLike, coc2x:int, coc2y:int, coc2x_pixel:int, coc2y_pixel:int) -> npt.ArrayLike:
        """Light check series trans to  coc2 correspond area, and append to new array (4-D).
        
        example 
        
        First Step: (480, 270) transpose to (270, 480)
        
        Second Step: get each size area from MODEL_NO by product.
        
        Third Step: Append the each area array to new array
        
        If the area is A, insert array to new_arr[0][0], etc.

        Parameter:
        ----------
            coc2x: SW_X_COC2
            coc2y: SW_Y_COC2
            coc2x_pixel: COC2_X_PIXEL
            coc2y_pixel: COC2_Y_PIXEL
        """
        arr = arr.T 
        area_np_arr = np.zeros((coc2x ,coc2y , coc2x_pixel, coc2y_pixel), dtype=int)
        for y in range(coc2x):
            for x in range(coc2y):
                area_np_arr[y][x] = arr[y*coc2x_pixel:(y+1)*coc2x_pixel, x*coc2y_pixel:(x+1)*coc2y_pixel]
        return area_np_arr
        

    def get_specific_area(self, lightingArray: npt.ArrayLike) -> npt.ArrayLike:
        """Create lighting check 4D array by MODEL_NO.

        Parameter:
        ----------
            npt.ArrayLike: lightingArray

        Returns:
            array: 4-D array
        """

        # try:
        if self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'): 
            each_coc2 = self.specific_area_arr(
                lightingArray, T161FUN01["SW_Y_COC2"], T161FUN01["SW_X_COC2"], T161FUN01["COC2_Y_PIXEL"],T161FUN01["COC2_X_PIXEL"]
            )
            return each_coc2
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('13.6'):
            each_coc2 = self.specific_area_arr( 
                lightingArray, T136FUN01["SW_X_COC2"], T136FUN01["SW_Y_COC2"], T136FUN01["COC2_X_PIXEL"], T136FUN01["COC2_Y_PIXEL"]
            )
            return each_coc2
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('17.3'):
            each_coc2 = self.specific_area_arr(
                lightingArray, T173XUN01["SW_X_COC2"], T173XUN01["SW_Y_COC2"], T173XUN01["COC2_X_PIXEL"], T173XUN01["COC2_Y_PIXEL"]
            )
            return each_coc2
        # except Exception as e:
        #     logging.error(str(e))
        


    def Full_Bonding_Matrix(self, bonding_df:pd.DataFrame, each_coc2:npt.ArrayLike, LED_Type:str) -> npt.ArrayLike:
        """Insert the value of each corresponding coc2 area to a 4-D zeros matrix. Then, return a completely bonding
        matrix (It has area A, B, C, D, E, etc).
        
        Parameter:
        ----------
            bonding_df (pd.DataFrame): dataframe in period of time
            
            each_coc2 (npt.ArrayLike): 4-D array
            
            LED_Type (str): R or G or B

        Returns:
            npt.ArrayLike: fully_bond_matrix
        """
        db = self.client[self.DBname]
        bond_fs = gridfs.GridFS(db, collection='BondSummaryTable')
          
        if "-" in self.SHEET_ID:
            UpdateAOI_SHEET_ID = self.SHEET_ID.split("-")[0]
            sheetID_df = bonding_df[bonding_df["SHEET_ID"]==UpdateAOI_SHEET_ID]
            del UpdateAOI_SHEET_ID
        else:
            sheetID_df = bonding_df[bonding_df["SHEET_ID"]==self.SHEET_ID]  
            
        by_led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
        bonding_2D_path = by_led_type_df["Bonding_Matrix"].tolist()
        Target_Area_No = by_led_type_df["Target_Area_No"].tolist()
        fully_bond_matrix = np.zeros_like(each_coc2)

        for b2Dp, area in zip(bonding_2D_path, Target_Area_No):
            try:
                b2D = bond_fs.get(ObjectId(b2Dp)).read()
                b2D = pickle.loads(b2D)
                if self.MODEL_NO in self.MODEL_TYPE_DIC.get('13.6'):
                    dict136 = self.coc2_area_dict(types='13.6')
                    fully_bond_matrix[dict136.get(area)[0]][dict136.get(area)[1]] = b2D
                elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'):
                    dict161 = self.coc2_area_dict(types='16.1')
                    fully_bond_matrix[dict161.get(area)[0]][dict161.get(area)[1]] = b2D
                elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('17.3'):
                    dict173 = self.coc2_area_dict(types='17.3')
                    fully_bond_matrix[dict173.get(area)[0]][dict173.get(area)[1]] = b2D    
            except:
                continue        
        return fully_bond_matrix 
    
    
    def full_Yield_Check(self, fully_bond_matrix: npt.ArrayLike, each_coc2: npt.ArrayLike, LED_TYPE:str) -> int:
        """Return Yield array.

        Parameter:
        ----------
            fully_bond_matrix (npt.ArrayLike)
            reshpeLightingArray (npt.ArrayLike)
        Returns:
            npt.ArrayLike: Array including process NG, process OK, no_process_ok, no_process_ng
        """
        # 以 bonding 以及整片的大小(480*270)角度來看
        res = fully_bond_matrix + each_coc2
        process_ok = np.count_nonzero(res==11)
        process_ng = np.count_nonzero(res==10)
        no_process_ok = np.count_nonzero(res==1)
        no_process_ng = np.count_nonzero(res==0)
        # dataframe 中有 BA0X, 代表有跳 pitch, 因為被跳過的pitch不能算NG, 所以數量需要另外計算
        if 'BA0X' in self.new_df['Defect_Code'].tolist():
            temp_cnt_df = self.new_df[(self.new_df['Defect_Code'] != 'BA0X') & (self.new_df['LED_TYPE'] == LED_TYPE)]
            if self.OPID in self.MB_history_types:
                process_ok = sum(np.where(temp_cnt_df['Lighting_check']=='1', 1, 0))
                process_ng = sum(np.where(temp_cnt_df['Lighting_check']=='0', 1, 0))
                no_process_ok = 0
                no_process_ng = 0 
            else:
                total = len(temp_cnt_df.index)
                no_process_ng = total - process_ok - process_ng - no_process_ok
                
        else:        
            if self.OPID in self.MB_history_types:
                process_ok = no_process_ok
                process_ng = no_process_ng
                no_process_ok = 0
                no_process_ng = 0  
                  
        totalBond = process_ok + process_ng
        if totalBond == 0:
            BSR = 0
        else:
            value = float((process_ok/totalBond)*100)
            BSR = math.floor(value*100/100.0)
        return res, process_ok, process_ng, no_process_ok, no_process_ng, BSR


    def AreaYieldCheck(self, matrixList: npt.ArrayLike) -> int:
        """Calculate process_ok, process_ng, no_process_ok, no_process_ng count.

        Parameter:
        ----------
            matrixList (npt.ArrayLike): Yield matrix

        Returns:
            int: int
        """
        process_ok = np.count_nonzero(matrixList==11)
        process_ng = np.count_nonzero(matrixList==10)
        no_process_ok = np.count_nonzero(matrixList==1)
        no_process_ng = np.count_nonzero(matrixList==0)
        return process_ok, process_ng, no_process_ok, no_process_ng

    
    def create_complete_Bonding(self, need2processDF:pd.DataFrame, each_coc2:npt.ArrayLike, LED_Type:str, key) -> pd.DataFrame:
        """create complete Bonding dataframe include each coc2 yield and return it.
        It will be concated by after each for loop.

        Parameter:
        ----------
            pd.DataFrame: need2processDF
            npt.ArrayLike: each_coc2
            str: LED_Type

        Returns:
            pd.DataFrame: R or G or B Bonding Dataframe
        """
        
        AOICT = self.new_df.iat[15,0]
        db = self.client[self.DBname]
        bond_fs = gridfs.GridFS(db, collection='BondSummaryTable')
        
        if "-" in str(self.SHEET_ID):
            UpdateAOI_SHEET_ID = self.new_df.iat[2,0].split("=")[1].split("-")[0]
            reTestTime = self.new_df.iat[2,0].split("=")[1].split("-")[1]
            sheetID_df = need2processDF[need2processDF["SHEET_ID"]==UpdateAOI_SHEET_ID]
            del UpdateAOI_SHEET_ID
            led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
            led_type_df.insert(loc=2, column="Re_Test", value=reTestTime)    
            
        else:
            sheetID_df = need2processDF[need2processDF["SHEET_ID"]==str(self.SHEET_ID)]  
            if len(sheetID_df.index)==0:
                pass
            led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
            led_type_df.insert(loc=2, column="Re_Test", value=0)
        
        
        led_type_df.insert(loc=9, column="AOI_CreateTime", value=AOICT)
        LED_TYPE_df = led_type_df.copy()
        
        del led_type_df, AOICT
        
        # bonding 的 DataFrame 中, 在Bond的數量和矩陣 因為有時候會對不到
        # 所以都將其設為空字符串 ""
        # 在讀取時, 需要將其略過, 另外為了避免後面在assign column時 有index 長度對不上的問題
        # 所以會將可能有重複的datafame過濾掉
        
        LED_TYPE_df = LED_TYPE_df[(LED_TYPE_df['Bonding_Matrix'] != '')]
        
        duplicate_col = ['CreateTime', 'SHEET_ID', 'LED_TYPE', 'Target_Area_No', 'logon', 'logoff','Bond_COUNT', 'No_Bond_COUNT']
        
        LED_TYPE_df = LED_TYPE_df.drop_duplicates(duplicate_col, keep='last').reset_index(drop=True)
        bonding_2D_path = LED_TYPE_df["Bonding_Matrix"].tolist()
        Target_Area_No = LED_TYPE_df["Target_Area_No"].tolist()
         
        pok_ls, png_ls, nok_ls, npg_ls = [], [], [], []
        yield_arr_id_ls = []
        key_list = []
        for b2Dp, area in zip(bonding_2D_path, Target_Area_No):
            try:
                b2D = bond_fs.get(ObjectId(b2Dp)).read()
                b2D = pickle.loads(b2D)
                if self.MODEL_NO in self.MODEL_TYPE_DIC.get('13.6'):
                    dict136 = self.coc2_area_dict(types='13.6')
                    zeros_matrix = b2D + each_coc2[dict136.get(area)[0]][dict136.get(area)[1]]
                    yield_arr_id = self.fs.put(Binary(pickle.dumps(zeros_matrix, protocol=5)))
                    process_ok, process_ng, no_process_ok, no_process_ng = self.AreaYieldCheck(zeros_matrix)
                    
        
                elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'):
                    dict161 = self.coc2_area_dict(types='16.1')
                    zeros_matrix = b2D + each_coc2[dict161.get(area)[0]][dict161.get(area)[1]]
                    yield_arr_id = self.fs.put(Binary(pickle.dumps(zeros_matrix, protocol=5)))
                    process_ok, process_ng, no_process_ok, no_process_ng = self.AreaYieldCheck(zeros_matrix)
                    
                    
                elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('17.3'):
                    dict173 = self.coc2_area_dict(types='17.3')
                    zeros_matrix = b2D + each_coc2[dict173.get(area)[0]][dict173.get(area)[1]]
                    yield_arr_id = self.fs.put(Binary(pickle.dumps(zeros_matrix, protocol=5)))
                    process_ok, process_ng, no_process_ok, no_process_ng = self.AreaYieldCheck(zeros_matrix)
                    
                pok_ls.extend([process_ok])
                png_ls.extend([process_ng])
                nok_ls.extend([no_process_ok])
                npg_ls.extend([no_process_ng])
                yield_arr_id_ls.extend([yield_arr_id])
                key_list.extend([key])
                
            except:
                pok_ls.extend([''])
                png_ls.extend([''])
                nok_ls.extend([''])
                npg_ls.extend([''])
                yield_arr_id_ls.extend([''])
                key_list.extend([''])
     
        LED_TYPE_df = LED_TYPE_df.assign(
            inspection_type = key_list,
            Process_ok = pok_ls,
            Process_NG = png_ls,
            NO_Process_OK = nok_ls,
            No_Process_NG = npg_ls,
            AreaYield_2D_SavePath = yield_arr_id_ls
        )
        
        del pok_ls, png_ls, nok_ls, npg_ls, yield_arr_id_ls, Target_Area_No
        return LED_TYPE_df


    def concatRGBdf(self, R_BOND_DF, G_BOND_DF, B_BOND_DF) -> pd.DataFrame:
        SheetID_df = pd.concat([R_BOND_DF, G_BOND_DF, B_BOND_DF])
        return SheetID_df


    def getObjectID(self, LC_2D:npt.ArrayLike, defect_code_2D:npt.ArrayLike, LUM2D:npt.ArrayLike, 
                    CIEX_2D:npt.ArrayLike,CIEY_2D:npt.ArrayLike) -> ObjectId:
        """Return the ObjectID

        Returns:
            Lighting_Check_2D_array
            
            Defect_code_2D
            
            LUM2D 
        """
        # 轉置成 270, 480 後, 先上下再左右翻轉 以符合 light on 檢結果
        try:
            LC_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LC_2D.T, 0), 1), protocol=5)))
            DC_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(defect_code_2D.T, 0), 1), protocol=5)))
            LUM_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LUM2D.T, 0), 1), protocol=5)))
        except:
            LC_id, DC_id, LUM_id = '', '', ''
            
        CIEX_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(CIEX_2D.T, 0), 1), protocol=5)))
        CIEY_id = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(CIEY_2D.T, 0), 1), protocol=5)))
        
        return LC_id, DC_id, LUM_id, CIEX_id, CIEY_id


    def reshape_4d_arr_to_2d(self, coc2_arr, coc2_x) -> npt.ArrayLike:
        init_tuple = ()
        for y in range(coc2_x):
            e = np.concatenate(coc2_arr[y][:], axis=1)
            init_tuple += tuple(e)
        res_2d = np.vstack(init_tuple)
        return res_2d


    def get_2D_from_reshape_4D_arr(self, arr_4d):
        """Reshape coc2 4-D arr to 2-D arr by MODEL_NO and transpose.
        """
        if self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'): 
            res_2d = self.reshape_4d_arr_to_2d(arr_4d, T161FUN01["SW_Y_COC2"])
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('13.6'):
            res_2d = self.reshape_4d_arr_to_2d(arr_4d, T136FUN01["SW_X_COC2"])
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('17.3'):
            res_2d = self.reshape_4d_arr_to_2d(arr_4d, T173XUN01["SW_X_COC2"])
        return res_2d
    
    
    def Yield_2D_id(self, Rres:npt.ArrayLike, Gres:npt.ArrayLike, Bres:npt.ArrayLike):
        """Reshape 4-D array to 2-D, and write it to mongoDB.

        Parameter:
        ----------
            Rres (npt.ArrayLike)
            Gres (npt.ArrayLike)
            Bres (npt.ArrayLike)

        Returns:
            _id: mongodb id
        """
        self.reshape_following_data()
        RresReshapeTo2D = self.get_2D_from_reshape_4D_arr(Rres) # like (270, 480)
        GresReshapeTo2D = self.get_2D_from_reshape_4D_arr(Gres) # like (270, 480)
        BresReshapeTo2D = self.get_2D_from_reshape_4D_arr(Bres) # like (270, 480)
        RY = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(RresReshapeTo2D, 0), 1), protocol=5)))
        GY = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(GresReshapeTo2D, 0), 1), protocol=5)))
        BY = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(BresReshapeTo2D, 0), 1), protocol=5)))
        return RY, GY, BY
    
    
    def Chromaticity_2D(self, Chromaticity:str) -> npt.ArrayLike:
        """Return R & G & B Chromaticity 2D array (dtype is string).

        Parameters:
            Chromaticity (str): Chromaticity_X or Chromaticity_Y

        Returns:
            array
        """
        
        self.reshape_following_data()
        Chromaticity_R = np.asarray(self.R_df[Chromaticity][:], dtype=str)
        Chromaticity_G = np.asarray(self.G_df[Chromaticity][:], dtype=str)
        Chromaticity_B = np.asarray(self.B_df[Chromaticity][:], dtype=str)
            
        Chromaticity_R_arr = Chromaticity_R.reshape(self.height, self.width)
        Chromaticity_G_arr = Chromaticity_G.reshape(self.height, self.width)
        Chromaticity_B_arr = Chromaticity_B.reshape(self.height, self.width)
        
        return Chromaticity_R_arr, Chromaticity_G_arr, Chromaticity_B_arr 
    
    
    def LightingCheck_2D(self, key:str) -> npt.ArrayLike:
        """Return lighting ckeck array by specific inspection type.

        Parameters:
            key (str): Inspection type

        Returns:
            array
        """
        try:
            self.reshape_following_data()
            LC_R = np.asarray(self.R_df[self.inspection_Lighting_check_dict.get(key)][:], dtype=int)
            LC_G = np.asarray(self.G_df[self.inspection_Lighting_check_dict.get(key)][:], dtype=int)
            LC_B = np.asarray(self.B_df[self.inspection_Lighting_check_dict.get(key)][:], dtype=int)
            RlightingArray = LC_R.reshape(self.height, self.width)
            GlightingArray = LC_G.reshape(self.height, self.width)
            BlightingArray = LC_B.reshape(self.height, self.width)
        except:
            RlightingArray, GlightingArray, BlightingArray = '', '', '' 
        return RlightingArray, GlightingArray, BlightingArray   
    

    def Luminance_2D(self, key:str) -> npt.ArrayLike:
        """Return Luminance_2D array id by specific inspection type.

        Parameters:
            key (str): Inspection type

        Returns:
            _id
        """
        try:
            self.reshape_following_data()
            R_lum_2d = np.asarray(self.R_df[self.inspection_Luminance_dict.get(key)][:], dtype=float)
            G_lum_2d = np.asarray(self.G_df[self.inspection_Luminance_dict.get(key)][:], dtype=float)
            B_lum_2d = np.asarray(self.B_df[self.inspection_Luminance_dict.get(key)][:], dtype=float)
            R_lum_2d = R_lum_2d.reshape(self.height, self.width)
            G_lum_2d = G_lum_2d.reshape(self.height, self.width)
            B_lum_2d = B_lum_2d.reshape(self.height, self.width)
        except:
            R_lum_2d, G_lum_2d, B_lum_2d = '', '', '' 
        return R_lum_2d, G_lum_2d, B_lum_2d


    def defect_code_2D(self, key:str) -> npt.ArrayLike:
        """Return defect code 2D array by specific inspection type.

        Parameter:
        -----------
            key (str): Inspection type

        Returns:
            array
        """
        try:
            self.reshape_following_data()
            R_DC = np.asarray(self.R_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str)
            G_DC = np.asarray(self.G_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str)
            B_DC = np.asarray(self.B_df[self.inspection_Defect_Code_dict.get(key)][:], dtype=str)
            R_DEFECT_CODE_2D = R_DC.reshape(self.height, self.width)
            G_DEFECT_CODE_2D = G_DC.reshape(self.height, self.width)
            B_DEFECT_CODE_2D = B_DC.reshape(self.height, self.width)
        except:
            R_DEFECT_CODE_2D, G_DEFECT_CODE_2D, B_DEFECT_CODE_2D = '', '', ''
        return R_DEFECT_CODE_2D, G_DEFECT_CODE_2D, B_DEFECT_CODE_2D
        

    def assign_col(self, df_group_tmp, **kwargs) -> pd.DataFrame:
        """Assign value to Summary Table dataframe and specific column
        
        Returns:
            pd.DataFrame: assigned dataframe 
        """

        new_df = df_group_tmp.copy()
        new_df = new_df.assign(
            Process_OK = kwargs.get('Process_OK', ''),
            Process_NG = kwargs.get('Process_NG', ''),
            NO_Process_OK = kwargs.get('NO_Process_OK', ''),
            NO_Process_NG = kwargs.get('NO_Process_NG', ''),
            Bond_Success_Rate = kwargs.get('Bond_Success_Rate', ''),
            LightingCheck_2D = kwargs.get('LightingCheck_2D', ''),
            DefectCode_2D = kwargs.get('DefectCode_2D', ''),
            Luminance_2D = kwargs.get('Luminance_2D', ''),
            YiledAnalysis_2D = kwargs.get('YiledAnalysis_2D', ''), 
            Chromaticity_X_2D = kwargs.get('Chromaticity_X_2D', ''),
            Chromaticity_Y_2D= kwargs.get('Chromaticity_Y_2D', ''),
            HeatMap=0,
        )
        return new_df
    
    
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
        
        
    def Bonding_DB_change_into_dataframe(self, DBname:str, collection:str) -> pd.DataFrame:
        """Get Bonding dataframe only have CreateTime, SHEET_ID, LED_TYPE cloumns.
        Parameter:
        ----------
            DBname (str): name of data base
            collection (str): name of collection

        Returns:
            df: dataframe
        """
        db = self.client[DBname]
        collection = db[collection]
        cursor = collection.find({})
        df = pd.DataFrame.from_records(cursor)
        return df



class csv_pre_filter(Summary_produce, bonding_processing):
    """Pre filter csv file by MODEL_NO"""
        
    
    def __call__(self, file_list, temp_AOI_range_csv_name):
        self.file_list = sorted(file_list, key=lambda t: os.stat(t).st_mtime)
        self.temp_AOI_range_csv_name = temp_AOI_range_csv_name
        filter_data_list = self.check_model_no()
        return filter_data_list
    
    
    def process_row_data(self) -> pd.DataFrame:
        """Process the row csv and rename the all columns

        Parameter:
        ----------
            file_path (str): io

        Returns:
            pd.DataFrame
        """
        
        # 雖然csv檔案的儲存空間比較小...但在讀取時會很耗時
        # 若改用pkl檔儲存，儲存空間會比csv多2倍以上，但在解檔時，速度會比解csv檔快10倍
        try:
            self.new_df = pd.read_csv(self.file, sep='dialect', engine='python', encoding="utf-8-sig")
        except:
            self.new_df = pd.read_csv(self.file, sep='dialect', engine='python', encoding="latin-1")

        self.new_df = self.new_df['_FACTOR'].str.split(',', expand=True)
        
        # 將不足目標column數的dataframe補齊columns數，並更改column name。
        if len(self.new_df.columns) < len(self.column_list):
            gap = len(self.column_list) - len(self.new_df.columns)
            for i in range(gap):
                self.new_df[f'{50+i}'] = 0
        self.new_df.columns = self.column_list
    
    
    def RGB_df_FromRowData(self) -> None:
        self.R_df = self.new_df[self.new_df['LED_TYPE']=='R'].reset_index(drop=True)
        self.G_df = self.new_df[self.new_df['LED_TYPE']=='G'].reset_index(drop=True)
        self.B_df = self.new_df[self.new_df['LED_TYPE']=='B'].reset_index(drop=True)

        
    def reshape_following_data(self) -> int:
        if len(self.R_df.index) != 0:
            height, width = max(self.R_df['LED_Index_I'].astype('int')), max(self.R_df['LED_Index_J'].astype('int'))
        elif len(self.G_df.index) != 0:
            height, width = max(self.G_df['LED_Index_I'].astype('int')), max(self.G_df['LED_Index_J'].astype('int'))
        elif len(self.B_df.index) != 0:
            height, width = max(self.B_df['LED_Index_I'].astype('int')), max(self.B_df['LED_Index_J'].astype('int'))
        return height, width


    def check_model_no(self) -> list:
        """Check file's MODEL_NO and there width and height.

        Returns:
            list: the file list pass the condiction 
        """
        
        print("[INFO] Checking File's MODEL_NO and Size...")
        
        checked_file_list = []
        df_list = []

    
        for self.file in self.file_list:
            
            start_t = time.time()
            # 使用多執行序可提升讀取速度約4~8%
            t0 = threading.Thread(target=self.process_row_data)
            t0.start()
            t0.join()
            print(f't0 cost {(time.time() - start_t):.4f} secs')
            
            del start_t
            
            t1 = threading.Thread(target=self.RGB_df_FromRowData)
            t1.start()
            t1.join()
            
            height, width = self.reshape_following_data()
            self.MODEL_NO = self.new_df.iloc[3,0].split("=")[-1] # get MODEL_NO 
            
            logging.warning(f"[INFO] Checking {self.file}")
        
            
            if self.MODEL_NO != '':
                standard_h, standard_w = self.get_h_w_info()
                
                if height != standard_h or width != standard_w:
                    
                    subject = f'MODEL_NO {self.MODEL_NO} Warning'
                    
                    mail_message = (
                        f"[Warning] MODEL_NO = {self.MODEL_NO}<br>\
                          [Warning] {self.file}<br>\
                          [Warning] Height = {height} & Width = {width}"
                    )
                    
                    def change_message_type(message:str, HTML_fomat:bool):
                        if HTML_fomat:
                            return message.replace('\n', '<br>')
                        return message.replace('<br>', '\n')
                        
                    logging.warning(change_message_type(mail_message, HTML_fomat=False))

                    send_message = customMessageAutoMail()
                    t2 = threading.Thread(target=send_message, args=(subject, mail_message))
                    t2.start()
                    t2.join()
                    
                    del subject, mail_message
                    
                else:
                    # Create temp dataframe from period of time
                    new_df_tmp = self.new_df[self.new_df['LED_TYPE']=='R'].copy()
                    df_group = new_df_tmp.groupby(['CreateTime'])[["Target_Carrier_ID"]].first().reset_index()
                    
                    del new_df_tmp
                    
                    df_list.append(df_group)
                    checked_file_list.append(self.file)
        
                    del df_group
                    
                del self.new_df, self.R_df, self.G_df, self.B_df, standard_h, standard_w
               
        try:
            rangeFile = pd.concat(df_list)
            pd.DataFrame(rangeFile).to_csv(f"./{self.temp_AOI_range_csv_name}", mode='w') 
            print(f"[INFO] {self.temp_AOI_range_csv_name} has created.")
        except Exception as e:
            logging.warning(str(e))
    
        return checked_file_list
        


class Yield_2D_array_defect_to_mongodb(Summary_produce):
    """Write the Yield 2D array, inspection type, LED type, decfect coordinate to mongoDB.
    """
    def __init__(self, df:pd.DataFrame, SHEET_ID:str, CreateTime:str, LED_TYPE:str, yield_4D_array:npt.ArrayLike, 
                 Luminance_array:npt.ArrayLike, OPID:str, MODEL_NO:str, key:str):
        super().__init__()
        # 判斷當前處理檔案的條件以及檢測條件
        self.df = df
        self.SHEET_ID = SHEET_ID
        self.CreateTime = CreateTime
        self.LED_TYPE = LED_TYPE
        self.yield_4D_array = yield_4D_array
        self.Luminance_array = Luminance_array
        self.OPID = OPID
        self.MODEL_NO = MODEL_NO
        self.Insepction_Type = key
        self.defect_df = pd.DataFrame()
        self.NG_TFT_DB_name = 'MT'
        # got NG TFT info collection
        self.NG_TFT_Collection_name = 'COC2_AOI'
        self.NG_TFT_Collection_ng = 'COC2_AOI_INDEX'
        self.NG_TFT_Collection_arr = 'COC2_AOI_ARRAY'
        
        self.need_to_drop_columns = [
            'LED_Coordinate_X', 'LED_Coordinate_Y', 'CIE1931_Chromaticity_X', 'CIE1931_Chromaticity_Y',
            'L0_Lighting_check', 'Defect_Code_L0', 'Lighting_check',
            'L10_Lighting_check', 'Defect_Code_L10', 'edge_Dark_point'
        ]
        self.reorder_columns_list = [
            'CreateTime', 'SHEET_ID', 'OPID', 'LED_TYPE', 'Insepction_Type', 'Pixel_X', 'Pixel_Y', 'Luminance'
        ]
        self.duplicate_columns_list = [
            'CreateTime', 'SHEET_ID', 'OPID', 'LED_TYPE', 'Insepction_Type',
            'Pixel_X', 'Pixel_Y', 'Luminance'
        ]
        
        self.insert_dataframe_to_mongoDB()
    
    
    def reshapeLightingArray(self):
        """Reshape lighting 2d array MODEL_NO like:
        
        (3, 2, 160, 135)  to (480, 270)
        
        """
        if self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'): 
            self.yield_4D_array = self.reshape_4d_arr_to_2d(self.yield_4D_array, T161FUN01["SW_Y_COC2"])
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('13.6'):
            self.yield_4D_array = self.reshape_4d_arr_to_2d(self.yield_4D_array, T136FUN01["SW_X_COC2"])
        elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('17.3'):
            self.yield_4D_array = self.reshape_4d_arr_to_2d(self.yield_4D_array, T173XUN01["SW_X_COC2"])
        return self.yield_4D_array.T # 將矩陣從 270*480 轉回 480*270
    
        
    def get_df_ng_coordinates(self):
        """Get defect x, y coordinate from dataframe and lighting check is 0
        """
        self.df = self.df[self.df['Defect_Code'] != 'BA0X'].reset_index(drop=True)
        self.ng_df = self.df[self.df['Lighting_check'].astype(str) == '0']
        self.x_ng_coord = self.ng_df['LED_Index_I'].astype(int).tolist()
        self.y_ng_coord = self.ng_df['LED_Index_J'].astype(int).tolist()
        self.defect_df['Pixel_X'] = self.x_ng_coord
        self.defect_df['Pixel_Y'] = self.y_ng_coord
    
    
    def get_lumiance_value_from_coord(self):
        self.get_df_ng_coordinates()
        lumiance_coord_value = [self.Luminance_array[i-1, j-1] for i, j in zip(self.x_ng_coord, self.y_ng_coord)]
        self.defect_df['Luminance'] = lumiance_coord_value
        del lumiance_coord_value
               
               
    def CreateSummaryTable(self):
        self.get_lumiance_value_from_coord()
        
        self.defect_df['CreateTime'] = [self.CreateTime for _ in range(len(self.defect_df.index))]
        self.defect_df['SHEET_ID'] = [self.SHEET_ID for _ in range(len(self.defect_df.index))]
        self.defect_df['OPID'] = [self.OPID for _ in range(len(self.defect_df.index))]
        self.defect_df['Insepction_Type'] = [self.Insepction_Type for _ in range(len(self.defect_df.index))]
        self.defect_df['LED_TYPE'] = [self.LED_TYPE for _ in range(len(self.defect_df.index))]
        self.defect_df = self.defect_df[self.reorder_columns_list]
        self.defect_df.drop_duplicates(self.duplicate_columns_list, keep='last').reset_index(drop=True)
        return self.defect_df
    
    
    def insert_dataframe_to_mongoDB(self):
        self.defect_df = self.CreateSummaryTable()
        # connect to MongoDB
        db = self.client['TEST']
        collection = db['AOI_LUM_Defect_Coordinates'] 
        
        # insert to Mongo DB
        if len(self.defect_df.index) != 0:
            if collection not in self.client.list_database_names():
                collection.create_index([("SHEET_ID", 1), ("OPID", 1), ("LED_TYPE", 1), ("Insepction_Type", 1)])
                result = self.defect_df.to_json(orient="records") 
            else:
                result = self.defect_df.to_json(orient="values")
            parsed = json.loads(result)
            operation = [InsertOne(doc) for doc in parsed]
            collection.bulk_write(operation)
        else:
            logging.warning(f'{self.SHEET_ID} in LED_TYPE {self.LED_TYPE} No Defect')

    
    

        

        

        
