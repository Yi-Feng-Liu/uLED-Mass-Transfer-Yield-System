from typing import Any
import pandas as pd
import numpy as np
from utils._config_ import T161FUN01, T136FUN01, T173XUN01, V130FLN02
import numpy.typing as npt
import logging
from utils._BondingSummaryProduction_ import bonding_processing
from utils.sendMail import alarmAutoMail, customMessageAutoMail
import json
from bson.binary import Binary
from pymongo import MongoClient, InsertOne
import pickle
import gridfs
import os
from bson import ObjectId
import math


class Summary_produce(bonding_processing):
    """Write the dataframe to MonogDB
    """
    def __init__(self):
        super().__init__()
        self.DBname = 'TEST'
        self.collection_name = 'LUM_SummaryTable'
        self.client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017/?compressors=zlib')
        self.db = self.client[self.DBname]
        self.fs = gridfs.GridFS(self.db, self.collection_name)
        self.inspection_type_list = ['L255', 'L10', 'L0', 'edge_Dark_point']
        
        # edge_Dark_point 的 2D，用L255代替
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
        self.width_height_dic = {
            'T136FUN01.0':{
                'h': T136FUN01["COC2_Y_PIXEL"]*T136FUN01["SW_Y_COC2"],
                'w': T136FUN01["COC2_X_PIXEL"]*T136FUN01["SW_X_COC2"],
            },
            'T173XUN01.1':{
                'h': T173XUN01["COC2_Y_PIXEL"]*T173XUN01["SW_Y_COC2"],
                'w': T173XUN01["COC2_X_PIXEL"]*T173XUN01["SW_X_COC2"],
            },
            'T161FUN01.0':{
                'h': T161FUN01["COC2_X_PIXEL"]*T161FUN01["SW_X_COC2"],
                'w': T161FUN01["COC2_Y_PIXEL"]*T161FUN01["SW_Y_COC2"],
            },
            'V130FLN02':{
                'h': V130FLN02["COC2_Y_PIXEL"]*V130FLN02["SW_Y_COC2"],
                'w': V130FLN02["COC2_X_PIXEL"]*V130FLN02["SW_X_COC2"]
            },
            'Y173EUN01':{
                'h': T173XUN01["COC2_Y_PIXEL"]*T173XUN01["SW_Y_COC2"],
                'w': T173XUN01["COC2_X_PIXEL"]*T173XUN01["SW_X_COC2"],
            },
            'Y136FLN03.0':{
                'h': T136FUN01["COC2_Y_PIXEL"]*T136FUN01["SW_Y_COC2"],
                'w': T136FUN01["COC2_X_PIXEL"]*T136FUN01["SW_X_COC2"],
            },
            'T161XUN01.0':{
                'h': T161FUN01["COC2_X_PIXEL"]*T161FUN01["SW_X_COC2"],
                'w': T161FUN01["COC2_Y_PIXEL"]*T161FUN01["SW_Y_COC2"]
            }
        }
        # product 13.6 and product 13.0 are the same size (480, 270)
        self.MODEL_TYPE_DIC = {
            '13.6': ['T136FUN01.0', 'Y136FLN03.0', 'V130FLN02'],
            '16.1': ['T161XUN01.0', 'T161FUN01.0'],
            '17.3': ['T173XUN01.1', 'Y173EUN01']
        }
        self.MODEL_NO = ''
        self.SHEET_ID = ''
        self.OPID = ''
        self.w = 0
        self.h = 0

            
    def process_row_data(self, file:str):
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
     

    def rename_new_df_and_turn_in_column(self):
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
       


    def RGB_df_FromRowData(self):
        self.rename_new_df_and_turn_in_column()
        self.R_df = self.new_df[self.new_df['LED_TYPE']=='R']
        self.R_df = self.R_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
        self.G_df = self.new_df[self.new_df['LED_TYPE']=='G']
        self.G_df = self.G_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
        self.B_df = self.new_df[self.new_df['LED_TYPE']=='B']
        self.B_df = self.B_df.sort_values(by=['LED_Index_I', 'LED_Index_J']).reset_index(drop=True)
    
    
    def get_return_dataframe(self):
        """return Dataframe need to be pass in function.

        Returns:
            self.new_df, self.R_df, self.G_df, self.B_df: pd.DataFrame
        """
        self.RGB_df_FromRowData()
        return self.new_df, self.R_df, self.G_df, self.B_df
    
        
    def assignCommonValue(self):
        """The common value means the dataframe of RGB they are have same columns

        Parameter:
        ----------
            df : the dataframe you input to the func

        Returns:
            columnlist: list
        """
        # append recipe & opid
        
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
    
            
    def checkColumnList(self):
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


    def coc2_area_dict(self, types:str):
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


    def reshape_following_data(self):
        if len(self.R_df.index) != 0:
            self.height, self.width = self.R_df['LED_Index_I'].max(), self.R_df['LED_Index_J'].max()
        elif len(self.G_df.index) != 0:
            self.height, self.width = self.G_df['LED_Index_I'].max(), self.G_df['LED_Index_J'].max()
        elif len(self.B_df.index) != 0:
            self.height, self.width = self.B_df['LED_Index_I'].max(), self.B_df['LED_Index_J'].max() 
        return self.height, self.width


    def get_return_value(self):
        """
        Returns:
            SHEET_ID, MODEL_NO, OPID: str
        """
        _, _, self.SHEET_ID, self.MODEL_NO, _, self.OPID = self.checkColumnList()
        return self.SHEET_ID, self.MODEL_NO, self.OPID

    
    def calculate_yield_column(self, df:pd.DataFrame, ok_column_name:str, ng_column_name:str, key):
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
            self.w = self.width_height_dic.get(self.MODEL_NO)['w']
            self.h = self.width_height_dic.get(self.MODEL_NO)['h']
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
        # if key == 'edge_Dark_point':
        key_list3 = spec + ['OKCNT', 'TOTAL', 'Yield']
        # else:
        #     key_list3 = spec + ['TOTAL', 'Yield']
        df_group = df_group[key_list3]
        res_df = df_group_split_defect.merge(df_group, how='left', on=spec)
        res_df.rename(columns={
            self.inspection_Defect_Code_dict.get(key): 'Defect_Code',
            'ABBR_No': 'ABBR_NO',
        }, inplace=True)
        return res_df
        

    def CreateSummaryTable(self, RGBdf:pd.DataFrame, key:str, Grade:str):
        """Create Summary Table using part A information
        
        param:
            RGBdf (pd.DataFrame): R, G and B dataframe, respctively.
            
        Returns:
            pd.DataFrame: Summary Table dataframe 
        """
        TOOL_ID, MES_ID, self.SHEET_ID, self.MODEL_NO, ABBR_NO, self.OPID = self.checkColumnList()
        
        new_df_tmp = RGBdf.copy()
        new_df_tmp = new_df_tmp[new_df_tmp['Defect_Code'] != 'BA0X'].reset_index(drop=True)
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
            
        key_list = ['CreateTime', 'SHEET_ID', 'MES_ID', 'TOOL_ID', 'MODEL_NO', 'ABBR_NO', 'ACTUAL_RECIPE', 'OPID', 'LED_TYPE', 'Inspection_Type', 'Defect_Code', 'NGCNT', 'OKCNT', 'TOTAL', 'Yield']
        df_group_tmp = df_group[key_list]
        df_group_tmp = df_group_tmp.copy()
        df_group_tmp['Grade'] = Grade
        return df_group_tmp

    
    def specific_area_arr(self, arr:npt.ArrayLike, coc2x:int, coc2y:int, coc2x_pixel:int, coc2y_pixel:int):
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
        

    def get_specific_area(self, lightingArray: npt.ArrayLike):
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
        


    def Full_Bonding_Matrix(self, bonding_df:pd.DataFrame, each_coc2:npt.ArrayLike, LED_Type:str):
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
        
        # try:    
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
    
    
    def full_Yield_Check(self, fully_bond_matrix: npt.ArrayLike, each_coc2: npt.ArrayLike, LED_TYPE:str):
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
            if self.OPID == 'TNLBO' or self.OPID == 'UM-BON':
                process_ok = sum(np.where(temp_cnt_df['Lighting_check']=='1', 1, 0))
                process_ng = sum(np.where(temp_cnt_df['Lighting_check']=='0', 1, 0))
                no_process_ok = 0
                no_process_ng = 0 
            else:
                total = len(temp_cnt_df.index)
                no_process_ng = total - process_ok - process_ng - no_process_ok
                
        else:        
            if self.OPID == 'TNLBO' or self.OPID == 'UM-BON':
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


    def AreaYieldCheck(self, matrixList: npt.ArrayLike):
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

    
    def create_complete_Bonding(self, need2processDF:pd.DataFrame, each_coc2:npt.ArrayLike, LED_Type:str, key):
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
                logging.warning('Line 322 No correspond sheetID_df')
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
                    pok_ls.extend([process_ok])
                    png_ls.extend([process_ng])
                    nok_ls.extend([no_process_ok])
                    npg_ls.extend([no_process_ng])
                    yield_arr_id_ls.extend([yield_arr_id])
                    key_list.extend([key])
        
                elif self.MODEL_NO in self.MODEL_TYPE_DIC.get('16.1'):
                    dict161 = self.coc2_area_dict(types='16.1')
                    zeros_matrix = b2D + each_coc2[dict161.get(area)[0]][dict161.get(area)[1]]
                    yield_arr_id = self.fs.put(Binary(pickle.dumps(zeros_matrix, protocol=5)))
                    process_ok, process_ng, no_process_ok, no_process_ng = self.AreaYieldCheck(zeros_matrix)
                    pok_ls.extend([process_ok])
                    png_ls.extend([process_ng])
                    nok_ls.extend([no_process_ok])
                    npg_ls.extend([no_process_ng])
                    yield_arr_id_ls.extend([yield_arr_id])
                    key_list.extend([key])
                    
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
                    
                else:
                    logging.warning(f"MODEL_NO '{self.MODEL_NO}' can not be identified")
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


    def concatRGBdf(self, R_BOND_DF, G_BOND_DF, B_BOND_DF):
        SheetID_df = pd.concat([R_BOND_DF, G_BOND_DF, B_BOND_DF])
        return SheetID_df


    def getObjectID(self, LED_LC_Array:npt.ArrayLike, defect_code_2D:npt.ArrayLike, LUM2D:npt.ArrayLike):
        """Return the ObjectID

        Returns:
            Lighting_Check_2D_array
            
            Defect_code_2D
            
            LUM2D 
        """
        # 轉置成 270, 480 後, 先上下再左右翻轉 以符合 light on 檢結果
        try:
            LCsp = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LED_LC_Array.T, 0), 1), protocol=5)))
            DCsp = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(defect_code_2D.T, 0), 1), protocol=5)))
            LUMsp = self.fs.put(Binary(pickle.dumps(np.flip(np.flip(LUM2D.T, 0), 1), protocol=5)))
        except:
            LCsp, DCsp, LUMsp = '', '', ''
        return LCsp, DCsp, LUMsp


    def reshape_4d_arr_to_2d(self, coc2_arr, coc2_x):
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
    
    
    def LightingCheck_2D(self, key:str):
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
    

    def Luminance_2D(self, key:str):
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


    def defect_code_2D(self, key:str):
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
        

    def assign_col(self, df_group_tmp, **kwargs):
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
            Luminance_2D =kwargs.get('Luminance_2D', ''),
            YiledAnalysis_2D = kwargs.get('YiledAnalysis_2D', ''), 
            HeatMap=0,
            # Lighting_Rate = kwargs.get('Lighting_Rate', '')
        )
        return new_df
    
    
    def insert_dataframe_to_mongoDB(self, whole_df:pd.DataFrame, collection_name:str):
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
        
        
    def Bonding_DB_change_into_dataframe(self, DBname:str, collection:str):
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
    """Pre filter csv file by MODEL_NO
    """
    def __init__(self, file_list, temp_AOI_range_csv_name):
        super().__init__()
        self.file_list = sorted(file_list, key=lambda t: os.stat(t).st_mtime)
        self.temp_AOI_range_csv_name = temp_AOI_range_csv_name
 
    def RGB_df_FromRowData(self):
        self.R_df = self.new_df[self.new_df['LED_TYPE']=='R'].reset_index(drop=True)
        self.G_df = self.new_df[self.new_df['LED_TYPE']=='G'].reset_index(drop=True)
        self.B_df = self.new_df[self.new_df['LED_TYPE']=='B'].reset_index(drop=True)

        
    def reshape_following_data(self):
        if len(self.R_df.index) != 0:
            height, width = max(self.R_df['LED_Index_I'].astype('int')), max(self.R_df['LED_Index_J'].astype('int'))
        elif len(self.G_df.index) != 0:
            height, width = max(self.G_df['LED_Index_I'].astype('int')), max(self.G_df['LED_Index_J'].astype('int'))
        elif len(self.B_df.index) != 0:
            height, width = max(self.B_df['LED_Index_I'].astype('int')), max(self.B_df['LED_Index_J'].astype('int'))
        return height, width


    def process_row_data(self, file):
        """Process the row csv and rename the all columns

        Parameter:
        ----------
            file_path (str): io

        Returns:
            pd.DataFrame
        """
        try:
            df = pd.read_csv(file, sep='dialect', engine='python', encoding="utf-8-sig")
        except:
            df = pd.read_csv(file, sep='dialect', engine='python', encoding="latin-1")
        self.new_df = df['_FACTOR'].str.split(',', expand=True)
        del df
        
        # 將不足目標column數的dataframe補齊columns數，並更改column name。
        if len(self.new_df.columns) < len(self.column_list):
            gap = len(self.column_list) - len(self.new_df.columns)
            for i in range(gap):
                self.new_df[f'{50+i}'] = 0
        self.new_df.columns = self.column_list
        return self.new_df


    def check_model_no(self):
        """Check file's MODEL_NO and there width and height.

        Returns:
            list: the file list pass the condiction 
        """
        print("[INFO] Checking File's MODEL_NO and Size...")
        checked_file_list = []
        for file in self.file_list:
            self.new_df = self.process_row_data(file)
            self.RGB_df_FromRowData()
            height, width = self.reshape_following_data()
            self.MODEL_NO = self.new_df.iloc[3,0].split("=")[-1] # get MODEL_NO 
            logging.warning(f"[INFO] Checking {file}")
            if self.MODEL_NO != '':
                standard_h = self.width_height_dic.get(self.MODEL_NO)['h']
                standard_w = self.width_height_dic.get(self.MODEL_NO)['w']
                if height != standard_h or width != standard_w:
                    logging.warning(
                        f"[INFO] MODEL_NO and PRODUCT SIZE are not match.\n"
                        f"[INFO] The file height & width is {height}, {width}."
                    )
                    continue
                else:
                    checked_file_list.append(file)
            else:
                logging.warning(f'[INFO] MODEL_NO is empty')
                continue
        return checked_file_list


    def AOI_TimeRange(self):
        checked_file_list = self.check_model_no()
        print("[INFO] Creat AOI Time Summary...")
        df_list = []
        for ckfile in checked_file_list:
            try:
                df = pd.read_csv(ckfile, sep='dialect', engine='python', encoding="utf-8-sig")
            except:
                df = pd.read_csv(ckfile, sep='dialect', engine='python', encoding="latin-1")
            df = df['_FACTOR'].str.split(',', expand=True)
            new_df_tmp = df[df[6]=='R'].copy()
            new_df_tmp["SheetID"] = str(df.iat[2,0].split("=")[1])
            df_group = new_df_tmp.groupby([0])[["SheetID"]].first().reset_index()
            df_group.rename(columns = {0:'CreateTime'}, inplace = True)
            df_list.append(df_group)
        try:
            rangeFile = pd.concat(df_list)
            pd.DataFrame(rangeFile).to_csv(f"./{self.temp_AOI_range_csv_name}", mode='w') 
        except Exception as E:
            logging.error(str(E))
        del df_list 
        print(f"[INFO] {self.temp_AOI_range_csv_name} has created.")
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
        # len(self.defect_df.index) = len(self.x_ng_coord)
     
    
    # def get_NG_TFT_AOI_dataframe(self):
    #     db = self.client[self.NG_TFT_DB_name]
    #     collection = db[self.NG_TFT_Collection_name]
    #     self.fs = gridfs.GridFS(db, collection=self.NG_TFT_Collection_ng)
    #     cursor = collection.find(
    #         {'SHEET_ID':self.SHEET_ID, 'LED_TYPE':self.LED_TYPE}, 
    #         {'CreateTime':1, 'SHEET_ID':1, 'LED_TYPE':1, 'OPID':1, 'ng_info_id':1}
    #     )
    #     self.NG_TFT_AOI_df = pd.DataFrame.from_records(cursor)
        
        
    # def get_SHEET_ID_list(self):
    #     """Get the SHEET_ID of NG TFT AOI df
    #     """
    #     # 確認是否有這片SHEET ID
    #     self.get_NG_TFT_AOI_dataframe()
    #     self.TFT_AOI_SHEE_ID_list = []
    #     if len(self.NG_TFT_AOI_df.index) != 0:
    #         self.TFT_AOI_SHEE_ID_list = list(dict.fromkeys(self.NG_TFT_AOI_df['SHEET_ID'].tolist()))
    #     else:
    #         logging.warning(f'[WARNING] SHEET ID {self.SHEET_ID} NOT IN TFT DATABASE')
        
        
        
    # def get_sheet_id_dataframe(self):
    #     """Step_1. Find correspong the dataframe from processing init info.
    #     """
    #     self.get_SHEET_ID_list()
    #     self.TFT_sheet_df = pd.DataFrame()
    #     # 把 empty 的 opid 拿掉, 若有重測, 則選擇最新的資料
    #     if len(self.TFT_AOI_SHEE_ID_list) != 0:
    #         self.TFT_sheet_df = self.NG_TFT_AOI_df.sort_values(by=['CreateTime'], ascending=False)
    #         print(self.TFT_sheet_df, '\n')
            
    #         self.TFT_sheet_df = self.TFT_sheet_df[self.TFT_sheet_df['OPID'] != 'empty'].drop_duplicates(['SHEET_ID', 'LED_TYPE', 'OPID'], keep='first').reset_index(drop=True)
    #         print(self.TFT_sheet_df)
    #     return self.TFT_sheet_df
                
    
    # def get_bonding_dataframe(self):
    #     """Get bonding time from database MT and that collection BondSummaryTable
        
    #     The TFT createTime and processing createTime needs to in period of two bonding times.
    #     else, used the last time of bonding time to be the filter condiction.
    #     """
    #     db = self.client["MT"]
    #     collection = db["BondSummaryTable"]
    #     # 在 bonding 的 dataframe 中，找到目前正在處理的 sheet ID 是否存在於 bonding dataframe。
    #     cursor = collection.find({'SHEET_ID':self.SHEET_ID, 'LED_TYPE':self.LED_TYPE}, 
    #                              {'CreateTime':1, 'SHEET_ID':1, 'LED_TYPE':1})
    #     bonding_df = pd.DataFrame.from_records(cursor)
    #     if isinstance(bonding_df, type(None)):
    #         bonding_df = pd.DataFrame()
            
    #     if len(bonding_df.index) != 0:
    #         bonding_df['CreateTime'] = bonding_df['CreateTime'].astype(str)
    #         bonding_df = bonding_df.sort_values(by=['CreateTime'], ascending=False).reset_index(drop=True)
    #         # 刪除重複的資料
    #         bonding_df = bonding_df.drop_duplicates(['CreateTime', 'SHEET_ID', 'LED_TYPE'], keep='first').reset_index(drop=True)
    #     else:
    #         logging.warning(f'[WARNING] COLLECTION: BondSummaryTable NOT FOUND SHEET ID {self.SHEET_ID}')
    #     return bonding_df
        
        
    # def compare_createTime(self):
    #     """Step_2. Compare the createTime from TFT_sheet_df, then get the TFT defect coordinates and image url.
    #     """
    #     self.TFT_sheet_df = self.get_sheet_id_dataframe()
    #     bond_sheet_df = self.get_bonding_dataframe()
    #     self.TFT_time_df = pd.DataFrame()
    #     # 如果沒有bonding的資料 就找大於或小於 目前處理檔案的時間
    #     if len(bond_sheet_df.index) == 0:
    #         if len(self.TFT_sheet_df) != 0:
    #             self.TFT_time_df = self.TFT_sheet_df[(self.TFT_sheet_df['CreateTime'].astype(str) < self.CreateTime) | (self.TFT_sheet_df['CreateTime'].astype(str) > self.CreateTime)]
    #             return self.TFT_time_df
    #     else:
    #         time_ls = sorted(bond_sheet_df["CreateTime"].astype(str).tolist()) 
    #         del bond_sheet_df
    #         # 判斷 TFT 的 sheet ID dataframe 是否介於兩次 bonding 之間，或者大於第一次 bonding 時間。
    #         if len(self.TFT_sheet_df.index) != 0:
    #             print(self.TFT_sheet_df, '\n')
    #             for previous, current in zip(time_ls, time_ls[1:]):
    #                 # 確認 TFT_sheet_df 有哪些資料在兩次的 bonding 之間
    #                 self.TFT_time_df = self.TFT_sheet_df[(self.TFT_sheet_df['CreateTime'].astype(str) < current) & (self.TFT_sheet_df['CreateTime'].astype(str) > previous)]
                        
    #                 if len(self.TFT_time_df.index) != 0:
    #                     print(self.TFT_time_df)
    #                     return self.TFT_time_df
    #                 else:
    #                     # 如果沒有，則選擇大於第一次 bonding 時間，因為AOI和LUM的檢查順序不一定，所以選擇or判斷
    #                     self.TFT_time_df = self.TFT_sheet_df[(self.TFT_sheet_df['CreateTime'].astype(str) > previous)]
    #                     self.TFT_time_df = self.TFT_time_df[(self.TFT_time_df['CreateTime'].astype(str) < self.CreateTime) | (self.TFT_time_df['CreateTime'].astype(str) > self.CreateTime)]
    #                     print(self.TFT_time_df)
    #                     return self.TFT_time_df
    #         else:
    #             logging.warning('Not Found correspond sheet dataframe in line 891')
                
       
    
    # def get_image_url_list(self):
    #     """Get image_url by TFT defect position and LUM defect coordinates
    #     """
    #     self.TFT_time_df = self.compare_createTime() 
    #     self.get_df_ng_coordinates()
    #     if isinstance(self.TFT_time_df, type(None)):
    #         self.TFT_time_df = pd.DataFrame()
    #     tft_x_coord, tft_y_coord, tft_img_link, tft_defect_code = [], [], [], []
    #     OPID_ls = []
    #     self.defect_df = self.defect_df.reset_index(drop=True).copy()
        
    #     if len(self.TFT_time_df.index) != 0:
    #         AOI_OPID_ls = self.TFT_time_df['OPID'].tolist()
    #         # 每個 OPID 都有 ng_info_id, 所以得找出特定的 dataframe
    #         for OPID in AOI_OPID_ls:
    #             OPID_df = self.TFT_time_df[self.TFT_time_df['OPID']==OPID]
    #             ng_series = OPID_df.ng_info_id.reset_index(drop=True)
    #             for i in range(len(ng_series.index)):
    #                 each_row_dict_id = ng_series[i]
    #                 dic = self.fs.get(ObjectId(each_row_dict_id)).read()
    #                 infos = pickle.loads(dic)
    #                 for j in range(len(infos)):
    #                     tft_x_coord.append(infos[j].get('LED_Index_X'))
    #                     tft_y_coord.append(infos[j].get('LED_Index_Y'))
    #                     tft_defect_code.append(infos[j].get('Defect Reciepe'))
    #                     tft_img_link.append(infos[j].get('LINK'))
    #                     OPID_ls.append(OPID)        
    #         temp_tft_df = pd.DataFrame(columns=['AOI_OPID', 'Pixel_X', 'Pixel_Y', 'MAP'])  
    #         temp_tft_df['AOI_OPID'] = OPID_ls
    #         temp_tft_df['Pixel_X'] = tft_x_coord
    #         temp_tft_df['Pixel_Y'] = tft_y_coord
    #         temp_tft_df['MAP'] = tft_img_link
    #         temp_tft_df['Defect_Code'] = tft_defect_code
    #         temp_tft_df = temp_tft_df[temp_tft_df['Defect_Code']!='BA0X']
            
    #         # drop_duplicates 避免merge的時候 dataframe 被展開，導致對不上 defect 長度
    #         temp_tft_df.drop_duplicates(subset=['AOI_OPID', 'Pixel_X', 'Pixel_Y'], keep='last', inplace=True)
    #         self.defect_df = self.defect_df.merge(temp_tft_df, how='outer', on=['Pixel_X', 'Pixel_Y'])
    #         print(self.defect_df)
    #         self.defect_df = self.defect_df.dropna(subset=['Defect', 'Luminance'])
    #         print(self.defect_df)
    #         del tft_x_coord, tft_y_coord, tft_img_link, tft_defect_code, ng_series
    #         return self.defect_df 
    #     else:
    #         self.defect_df['AOI_OPID'] = ["" for _ in range(len(self.defect_df.index))]
    #         self.tft_img_link = ["" for _ in range(len(self.defect_df.index))]
    #         self.defect_df['MAP'] = self.tft_img_link
    #         self.defect_df['Defect_Code'] = ["" for _ in range(len(self.defect_df.index))]
    #         logging.warning('[INFO] self.TFT_time_df is None')
    #         return self.defect_df  
        
        
    # def get_defect_value_from_coord(self):
    #     """Get Defect value from dataframe coordinates.
    #     """
    #     self.get_df_ng_coordinates()
    #     self.yield_2d_array = self.reshapeLightingArray()
    #     defect_coord_value = [self.yield_2d_array[i-1, j-1] for i, j in zip(self.x_ng_coord, self.y_ng_coord)]
    #     self.defect_df['Defect'] = defect_coord_value
    #     del defect_coord_value
    
    
    def get_lumiance_value_from_coord(self):
        self.get_df_ng_coordinates()
        lumiance_coord_value = [self.Luminance_array[i-1, j-1] for i, j in zip(self.x_ng_coord, self.y_ng_coord)]
        self.defect_df['Luminance'] = lumiance_coord_value
        del lumiance_coord_value
               
               
    def CreateSummaryTable(self):
        # self.get_defect_value_from_coord()
        self.get_lumiance_value_from_coord()
        # self.defect_df = self.get_image_url_list()
        
        self.defect_df['CreateTime'] = [self.CreateTime for _ in range(len(self.defect_df.index))]
        self.defect_df['SHEET_ID'] = [self.SHEET_ID for _ in range(len(self.defect_df.index))]
        self.defect_df['OPID'] = [self.OPID for _ in range(len(self.defect_df.index))]
        self.defect_df['Insepction_Type'] = [self.Insepction_Type for _ in range(len(self.defect_df.index))]
        self.defect_df['LED_TYPE'] = [self.LED_TYPE for _ in range(len(self.defect_df.index))]
        # self.defect_df['Defect'] = self.defect_df['Defect'].astype(int)
        # self.defect_df['Failure Analysis'] = np.where(self.defect_df['Defect_Code']=='AB06', '缺晶', np.where(self.defect_df['Defect_Code']=='OK', 'LED已上件', ''))
        # self.defect_df['Solution'] = ''
        # self.defect_df['Short term Action'] = ''

        # if self.OPID == 'TNLBO':
        #     self.defect_df['Defect'] = np.where(self.defect_df['Defect']==0, 'LED上件不亮', 'LED上件有亮')
        # elif self.OPID == 'TRLRE':
        #     self.defect_df['Defect'] = np.where(self.defect_df['Defect']==10, 'Repair Fail', '新增暗點')
        # else:
        #     self.defect_df['Defect'] = np.where(self.defect_df['Defect']==10, 'Fail', '新增暗點') 
        # self.defect_df = self.defect_df[self.defect_df['Defect_Code'] != 'BA0X']
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

    
    

        

        

        
