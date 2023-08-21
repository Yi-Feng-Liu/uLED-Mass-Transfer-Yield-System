import logging
from utils._AOISummaryProduction_ import Summary_produce, Yield_2D_array_defect_to_mongodb
from utils._BondingSummaryProduction_ import bonding_processing, bond_summary
import pandas as pd
from bson.binary import Binary
import pickle
import gridfs
from pymongo import MongoClient
import threading
import numpy as np


def find_AOI_Previous_Time(AOI_TimeRange_df, Running_FileSHEET_ID, Running_File_CT):
    """From AOI_Summary.csv to get dataframe of AOI time range. 
    
    The csv file record the need to process sheet ID and its inspection time.
    
    """
    if "-" in Running_FileSHEET_ID:
        newRunningFileSHEET_ID = Running_FileSHEET_ID.split("-")[0]
        AOISheetDF = AOI_TimeRange_df.loc[AOI_TimeRange_df["Target_Carrier_ID"].str[:len(newRunningFileSHEET_ID)] == newRunningFileSHEET_ID].reset_index(drop=True)

    else:
        AOISheetDF = AOI_TimeRange_df.loc[AOI_TimeRange_df["Target_Carrier_ID"] == Running_FileSHEET_ID].reset_index(drop=True)
        
    realTimeLi = sorted(AOISheetDF["CreateTime"].tolist())

    for previous, current in zip(realTimeLi, realTimeLi[1:]):
        if str(current) == Running_File_CT:
            return str(previous)
    return "202305310000"


def filter_bond_df_by_AOI(RGBbonding_df, RunningFileSHEET_ID, RunningFileCT, PreviousTime):   
    """Have to process Bonding dataframe that time need to greater than previous AOI time but smaller
    the file's create time.
    
    Because between two bonding time, only have once AOI inspection, so the the bonding dataframe need to 
    greater than previous AOI time if we have duplicate sheets ID.
    """        
    
    if "-" in RunningFileSHEET_ID:
        RunningFileSHEET_ID = RunningFileSHEET_ID.split("-")[0]

    BondSheetDF = RGBbonding_df[RGBbonding_df["SHEET_ID"]==RunningFileSHEET_ID]
    
    if len(BondSheetDF.index) == 0:
        pass
        
    need2processDF = BondSheetDF[(BondSheetDF["CreateTime"]>PreviousTime) & (BondSheetDF["CreateTime"]<RunningFileCT)]
    del BondSheetDF
    
    return need2processDF
    
    
def getGridFS(DBname:str, collection_name:str):
    """Get the GridFS from mongodb

    Args:
        DBname (str): which database
        collection (str): which collection

    Returns:
        fs: GridFS
    """
    
    client = MongoClient('mongodb://user:account@10.88.26.102:27017')
    db = client[DBname]
    fs = gridfs.GridFS(db, collection=collection_name)
    
    return fs


def get_grade(df:pd.DataFrame):
    """Get Grade of SHEET_ID from spec"""
    
    if 'BA0X' in df['Defect_Code'].tolist():
        spec_df = df[df['Defect_Code'] != 'BA0X'].reset_index(drop=True)
        total = len(spec_df.index)
        light_cnt = spec_df['Lighting_check'].astype(int).sum()
        light_yield = light_cnt/total
        
    else:    
        total = len(df.index)
        light_cnt = df['Lighting_check'].astype(int).sum()
        light_yield = light_cnt/total
    
    if light_yield >= 0.9999:
        return 'Z'
    
    elif light_yield >= 0.995 and light_yield < 0.9999:
        return 'P'
    
    elif light_yield >= 0.95 and light_yield < 0.995:
        return 'N'
    
    elif light_yield < 0.95:
        return 'S'


def AOI_main_procedure(AOIfile_path, key, temp_AOI_sheet_time_df):
    sp = Summary_produce()
    sp.process_row_data(AOIfile_path)
    
    AOI_CorresBond_SheetID_df = pd.DataFrame()
    
    SHEET_ID, MODEL_NO, OPID = sp.get_return_value()
    # print(SHEET_ID)
    logging.warning(f"Running {SHEET_ID} Ins type {key}")
  
    repair_df = sp.Bonding_DB_change_into_dataframe(DBname='TEST', collection='BondSummaryTable')
    # AOI_TimeRange_df = pd.read_csv(f"./{temp_AOI_range_csv_name}", dtype=str)
    new_df, R_df, G_df, B_df = sp.get_return_dataframe()
    grade = get_grade(new_df)
    Running_File_CT = new_df.iat[15,0]
    del new_df
    
    R_bond_df = repair_df[repair_df["LED_TYPE"]=="R"]
    G_bond_df = repair_df[repair_df["LED_TYPE"]=="G"]
    B_bond_df = repair_df[repair_df["LED_TYPE"]=="B"]
    del repair_df

    previousTime = find_AOI_Previous_Time(temp_AOI_sheet_time_df, SHEET_ID, Running_File_CT)
    del temp_AOI_sheet_time_df

    R_summary = sp.CreateSummaryTable(R_df, key, Grade=grade)
    G_summary = sp.CreateSummaryTable(G_df, key, Grade=grade)
    B_summary = sp.CreateSummaryTable(B_df, key, Grade=grade)
    del grade
    
    RDC2D, GDC2D, BDC2D = sp.defect_code_2D(key)
    R2DArray, G2DArray, B2DArray = sp.LightingCheck_2D(key)
    R_lum2D, G_lum2D, B_lum2D = sp.Luminance_2D(key)
    
    Chromaticity_Rx_arr, Chromaticity_Gx_arr, Chromaticity_Bx_arr = sp.Chromaticity_2D('CIE1931_Chromaticity_X')
    Chromaticity_Ry_arr, Chromaticity_Gy_arr, Chromaticity_By_arr = sp.Chromaticity_2D('CIE1931_Chromaticity_Y')
    
    # 判斷回傳值是否為空字符串 如果是則不處理
    if isinstance(R2DArray, np.ndarray) and isinstance(R_lum2D, np.ndarray):
        R_reshape_coc2 = sp.get_specific_area(R2DArray)
        G_reshape_coc2 = sp.get_specific_area(G2DArray)
        B_reshape_coc2 = sp.get_specific_area(B2DArray)
        
        R_need2process = filter_bond_df_by_AOI(R_bond_df, SHEET_ID, Running_File_CT, previousTime)
        G_need2process = filter_bond_df_by_AOI(G_bond_df, SHEET_ID, Running_File_CT, previousTime)
        B_need2process = filter_bond_df_by_AOI(B_bond_df, SHEET_ID, Running_File_CT, previousTime)
        del R_bond_df, G_bond_df, B_bond_df, previousTime
        
        # bond 的 coc2 也是 135, 160
        R_BondFullyMatrix = sp.Full_Bonding_Matrix(R_need2process, R_reshape_coc2, 'R')
        G_BondFullyMatrix = sp.Full_Bonding_Matrix(G_need2process, G_reshape_coc2, 'G')
        B_BondFullyMatrix = sp.Full_Bonding_Matrix(B_need2process, B_reshape_coc2, 'B')
        
        R_yield_4d, RPOK, RPNG, RNPOK, RNPNG, RBSR = sp.full_Yield_Check(R_BondFullyMatrix, R_reshape_coc2, LED_TYPE='R')
        G_yield_4d, GPOK, GPNG, GNPOK, GNPNG, GBSR = sp.full_Yield_Check(G_BondFullyMatrix, G_reshape_coc2, LED_TYPE='G')
        B_yield_4d, BPOK, BPNG, BNPOK, BNPNG, BBSR = sp.full_Yield_Check(B_BondFullyMatrix, B_reshape_coc2, LED_TYPE='B')  
        del R_BondFullyMatrix, G_BondFullyMatrix, B_BondFullyMatrix 
        
        RY_id, GY_id, BY_id = sp.Yield_2D_id(R_yield_4d, G_yield_4d, B_yield_4d)
        
        
        # 將 R, G, B Yield 2D 裡的 defect 座標，寫入MongoDB
        middle_task_1 = threading.Thread(target=Yield_2D_array_defect_to_mongodb, args=(R_df, SHEET_ID, Running_File_CT, 'R', R_yield_4d, R_lum2D, OPID, MODEL_NO, key))
        
        middle_task_2 = threading.Thread(target=Yield_2D_array_defect_to_mongodb, args=(G_df, SHEET_ID, Running_File_CT, 'G', G_yield_4d, G_lum2D, OPID, MODEL_NO, key))
        
        middle_task_3 = threading.Thread(target=Yield_2D_array_defect_to_mongodb, args=(B_df, SHEET_ID, Running_File_CT, 'B', B_yield_4d, B_lum2D, OPID, MODEL_NO, key))
        
        middle_task_1.start()
        middle_task_2.start()
        middle_task_3.start()
        
        # Yield_2D_array_defect_to_mongodb(R_df, SHEET_ID, Running_File_CT, 'R', R_yield_4d, R_lum2D, OPID, MODEL_NO, key)
        # Yield_2D_array_defect_to_mongodb(G_df, SHEET_ID, Running_File_CT, 'G', G_yield_4d, G_lum2D, OPID, MODEL_NO, key)
        # Yield_2D_array_defect_to_mongodb(B_df, SHEET_ID, Running_File_CT, 'B', B_yield_4d, B_lum2D, OPID, MODEL_NO, key)
        
        del R_yield_4d, G_yield_4d, B_yield_4d
        del R_df, G_df, B_df, SHEET_ID, OPID, MODEL_NO
        
        # 根據不同的檢測條件，
        R_bond_dataframe = sp.create_complete_Bonding(R_need2process, R_reshape_coc2, 'R', key)
        G_bond_dataframe = sp.create_complete_Bonding(G_need2process, G_reshape_coc2, 'G', key)
        B_bond_dataframe = sp.create_complete_Bonding(B_need2process, B_reshape_coc2, 'B', key)
        del R_need2process, G_need2process, B_need2process, R_reshape_coc2, G_reshape_coc2, B_reshape_coc2
        
        AOI_CorresBond_SheetID_df = sp.concatRGBdf(R_bond_dataframe, G_bond_dataframe, B_bond_dataframe)
        del R_bond_dataframe, G_bond_dataframe, B_bond_dataframe 
        
        RLCsp, RDCsp, RLUMsp, RChromaticity_x, RChromaticity_y = sp.getObjectID(R2DArray, RDC2D, R_lum2D, Chromaticity_Rx_arr, Chromaticity_Ry_arr)
        
        GLCsp, GDCsp, GLUMsp, GChromaticity_x, GChromaticity_y = sp.getObjectID(G2DArray, GDC2D, G_lum2D, Chromaticity_Gx_arr, Chromaticity_Gy_arr)
        
        BLCsp, BDCsp, BLUMsp, BChromaticity_x, BChromaticity_y = sp.getObjectID(B2DArray, BDC2D, B_lum2D, Chromaticity_Bx_arr, Chromaticity_By_arr)
        del R2DArray, G2DArray, B2DArray, RDC2D, GDC2D, BDC2D, R_lum2D, G_lum2D, B_lum2D

        R_summary = sp.assign_col(
            R_summary, LightingCheck_2D=RLCsp, DefectCode_2D=RDCsp, Luminance_2D=RLUMsp, YiledAnalysis_2D=RY_id, Process_OK=RPOK, Process_NG=RPNG, NO_Process_OK=RNPOK, NO_Process_NG=RNPNG, Bond_Success_Rate=RBSR, Chromaticity_X_2D=RChromaticity_x, Chromaticity_Y_2D=RChromaticity_y
        )
        
        G_summary = sp.assign_col(
            G_summary, LightingCheck_2D=GLCsp, DefectCode_2D=GDCsp, Luminance_2D=GLUMsp, YiledAnalysis_2D=GY_id, Process_OK=GPOK, Process_NG=GPNG, NO_Process_OK=GNPOK, NO_Process_NG=GNPNG, Bond_Success_Rate=GBSR, Chromaticity_X_2D=GChromaticity_x, Chromaticity_Y_2D=GChromaticity_y
        )
        
        B_summary = sp.assign_col(
            B_summary, LightingCheck_2D=BLCsp, DefectCode_2D=BDCsp, Luminance_2D=BLUMsp, YiledAnalysis_2D=BY_id, Process_OK=BPOK, Process_NG=BPNG, NO_Process_OK=BNPOK, NO_Process_NG=BNPNG, Bond_Success_Rate=BBSR, Chromaticity_X_2D=BChromaticity_x, Chromaticity_Y_2D=BChromaticity_y
        )

        whole_df = pd.concat([R_summary, G_summary, B_summary])
        sp.insert_dataframe_to_mongoDB(whole_df=whole_df, collection_name='LUM_SummaryTable')
        
        middle_task_1.join()
        middle_task_2.join()
        middle_task_3.join()   
        
    else:
        whole_df = pd.concat([R_summary, G_summary, B_summary])
        
    sp.insert_dataframe_to_mongoDB(whole_df=whole_df, collection_name='LUM_SummaryTable')
    
    del whole_df, R_summary, G_summary, B_summary
        
    return AOI_CorresBond_SheetID_df
    

def Bond_main_procedure(intersectionDataList):
    bp = bonding_processing()
    fs = getGridFS(DBname='TEST', collection_name='BondSummaryTable')
    # Process major data list
    for needProcessData in intersectionDataList:
        file_startswith = needProcessData.split("/")[-1].split("_")[1]
        if len(file_startswith) <= 4:
            continue
        else:
            majorData_df = bp.Bond_df(needProcessData)
            
            AreaNO = bp.getTargetAreaNo(majorData_df)
            CreateTime = bp.getCT(majorData_df)
            OPID = bp.getOPID(majorData_df)
            ToolID = bp.getToolID(majorData_df)
            ModelNo = bp.getModelNo(majorData_df)
            ABBR_No = bp.getABBR_No(majorData_df)
            EQP_RecipeID = bp.getEQP_Recipe_ID(majorData_df)
            SheetID = bp.getSheetID(majorData_df)
            Source_CarrierID = bp.getSource_CarrierID(majorData_df)
            RareaAssign = bp.areaMatrix(majorData_df)
            GareaAssign = bp.areaMatrix(majorData_df)
            BareaAssign = bp.areaMatrix(majorData_df)
            logon, logoff = bp.getLogOnOff(majorData_df)

            # Common columns
            common_value = [ToolID, SheetID, ModelNo, ABBR_No, OPID, EQP_RecipeID, Source_CarrierID, CreateTime, AreaNO, logon, logoff]
            R_df, G_df, B_df = bp.RGB_df(majorData_df)
            
            R_data_list = common_value.copy()
            if len(R_df.index)!=0:
                R = bp.getLED_Type(R_df)
                R_areaMatrix = bp.coordinateToMatrix(R_df, RareaAssign)
                R_bond_2D_id = fs.put(Binary(pickle.dumps(R_areaMatrix, protocol=5)))
                R_Process, R_no_Process = bp.areaQualityCount(R_areaMatrix)
                R_data_list.extend([R, R_Process, R_no_Process, R_bond_2D_id])
            else:
                R_data_list.extend(['R', "", "", ""])
                
            G_data_list = common_value.copy()
            if len(G_df.index)!=0:
                G = bp.getLED_Type(G_df)
                G_areaMatrix = bp.coordinateToMatrix(G_df, GareaAssign)
                G_bond_2D_id = fs.put(Binary(pickle.dumps(G_areaMatrix, protocol=5)))
                G_Process, G_No_Process = bp.areaQualityCount(G_areaMatrix)
                G_data_list.extend([G, G_Process, G_No_Process, G_bond_2D_id])
            else:
                G_data_list.extend(['G', "", "", ""])
            
            B_data_list = common_value.copy()
            if len(B_df.index)!=0:
                B = bp.getLED_Type(B_df)
                B_areaMatrix = bp.coordinateToMatrix(B_df, BareaAssign)    
                B_bond_2D_id = fs.put(Binary(pickle.dumps(B_areaMatrix, protocol=5)))
                B_Process, B_no_Process = bp.areaQualityCount(B_areaMatrix)
                B_data_list.extend([B, B_Process, B_no_Process, B_bond_2D_id])
            else:
                B_data_list.extend(['B', "", "", ""])

            bond_summary(DBname='TEST', collection_name='BondSummaryTable', R_data_list=R_data_list, G_data_list=G_data_list, B_data_list=B_data_list)
        



