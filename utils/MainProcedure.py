# import logging
# from utils._AOISummaryProduction_ import CreateLumSummary
# import gridfs
# from pymongo import MongoClient
# import numpy as np
# import pandas as pd




# def get_defectCount(df: pd.DataFrame) -> int:
#     if "BA0X" in df["Defect_Code"].unique():
#         df = df[df["Defect_Code"] != "BA0X"].reset_index(drop=True)
        
#     elif "AB0X" in df["Defect_Code"].unique():
#         df = df[df["Defect_Code"] != "AB0X"].reset_index(drop=True)

#     total = len(df.index)
#     light_cnt = df["Lighting_check"].astype(int).sum()

#     return total - light_cnt


# def get_GradeSpec(df: pd.DataFrame, minimumValue: int, middleValue: int, maximumValue: int) -> str:
#     defectCnt = get_defectCount(df)
#     if defectCnt == minimumValue:
#         return 'Z'
    
#     elif defectCnt > minimumValue and defectCnt < middleValue:
#         return 'P'
    
#     elif defectCnt >= middleValue and defectCnt < maximumValue:
#         return 'N'
    
#     elif defectCnt >= maximumValue:
#         return 'v'
    

# def get_Z300_GradeSpec(df: pd.DataFrame, minimumValue: int, middleValue: int, maximumValue: int) -> str:
#     defectCnt = get_defectCount(df)

#     if defectCnt > minimumValue and defectCnt <= middleValue:
#         return 'P'
    
#     elif defectCnt > maximumValue:
#         return 'N'
    

# def convFilter(ksize: int, arr:np.ndarray):
#     kernel = np.ones((ksize, ksize), dtype=np.uint8)
#     img = signal.convolve2d(arr, kernel, mode='valid')
#     return img
    
    
# def CreateSummary(
#     cls: object, 
#     R_df: pd.DataFrame, 
#     G_df: pd.DataFrame, 
#     B_df: pd.DataFrame, 
#     key: str,
#     CHIP: str,
#     haveChip: bool=False
# ) -> None: 
    
#     ksize = 4
#     ng_Four = ""
#     ng_eight = ""
#     R_summary = cls.CreateSummaryTable(R_df, key, CHIP=CHIP, haveChip=haveChip)
#     G_summary = cls.CreateSummaryTable(G_df, key, CHIP=CHIP, haveChip=haveChip)
#     B_summary = cls.CreateSummaryTable(B_df, key, CHIP=CHIP, haveChip=haveChip)
    
#     R_dc_arr, G_dc_arr, B_dc_arr = cls.defect_code_2D(key)
#     R_lc_arr, G_lc_arr, B_lc_arr = cls.LightingCheck_2D(key)
#     R_lum_arr, G_lum_arr, B_lum_arr = cls.Luminance_2D(key)
    
#     Chromaticity_Rx_arr, Chromaticity_Gx_arr, Chromaticity_Bx_arr = cls.Chromaticity_2D("CIE1931_Chromaticity_X")
#     Chromaticity_Ry_arr, Chromaticity_Gy_arr, Chromaticity_By_arr = cls.Chromaticity_2D("CIE1931_Chromaticity_Y")
    
#     # 判斷回傳值是否為空字符串 如果是則不處理
#     if isinstance(R_lc_arr, np.ndarray) and isinstance(R_lum_arr, np.ndarray):
#         # 亮點轉暗點
#         defect_arr_r = np.where(R_lc_arr == 1, 0, 1)
#         defect_arr_g = np.where(G_lc_arr == 1, 0, 1)
#         defect_arr_b = np.where(B_lc_arr == 1, 0, 1)
        
#         white = defect_arr_r + defect_arr_g + defect_arr_b
#         white = convFilter(ksize=ksize, arr=white)
#         ng_Four = np.count_nonzero(white >= 4)
#         ng_eight = np.count_nonzero(white >= 8)
        
#         RLC_ID, RDC_ID, RLUM_ID, RChromaticity_x, RChromaticity_y = cls.getObjectID(
#             R_lc_arr, R_dc_arr, R_lum_arr, Chromaticity_Rx_arr, Chromaticity_Ry_arr
#         )
        
#         GLC_ID, GDC_ID, GLUM_ID, GChromaticity_x, GChromaticity_y = cls.getObjectID(
#             G_lc_arr, G_dc_arr, G_lum_arr, Chromaticity_Gx_arr, Chromaticity_Gy_arr
#         )
        
#         BLC_ID, BDC_ID, BLUM_ID, BChromaticity_x, BChromaticity_y = cls.getObjectID(
#             B_lc_arr, B_dc_arr, B_lum_arr, Chromaticity_Bx_arr, Chromaticity_By_arr
#         )
#         del R_lc_arr, G_lc_arr, B_lc_arr, R_dc_arr, G_dc_arr, B_dc_arr, R_lum_arr, G_lum_arr, B_lum_arr

#         R_summary = cls.assign_col(
#             R_summary, LightingCheck_2D=RLC_ID, DefectCode_2D=RDC_ID, Luminance_2D=RLUM_ID, Chromaticity_X_2D=RChromaticity_x, Chromaticity_Y_2D=RChromaticity_y
#         )
        
#         G_summary = cls.assign_col(
#             G_summary, LightingCheck_2D=GLC_ID, DefectCode_2D=GDC_ID, Luminance_2D=GLUM_ID, Chromaticity_X_2D=GChromaticity_x, Chromaticity_Y_2D=GChromaticity_y
#         )
        
#         B_summary = cls.assign_col(
#             B_summary, LightingCheck_2D=BLC_ID, DefectCode_2D=BDC_ID, Luminance_2D=BLUM_ID, Chromaticity_X_2D=BChromaticity_x, Chromaticity_Y_2D=BChromaticity_y
#         )
        
#     whole_df = pd.concat([R_summary, G_summary, B_summary])
#     whole_df['Kernel'] = ksize
#     whole_df['4EA'] = ng_Four
#     whole_df['8EA'] = ng_eight
#     cls.insert_dataframe_to_mongoDB(whole_df=whole_df, collection_name="LUM_SummaryTable")
#     del whole_df, R_summary, G_summary, B_summary


# # def find_AOI_Previous_Time(LUM_temp_time_df, Running_SheetID, Running_File_CT):
# #     """From light_on_Summary.csv to get dataframe of LUM time range. 
    
# #     The csv file record the need to process sheet ID and its inspection time.
    
# #     """
# #     LUMSheetDF = LUM_temp_time_df.loc[LUM_temp_time_df["Target_Carrier_ID"] == Running_SheetID].reset_index(drop=True)
    
# #     timels = sorted(LUMSheetDF["CreateTime"].tolist())

# #     for previous, current in zip(timels, timels[1:]):
# #         if str(current) == Running_File_CT:
# #             return str(previous)
# #     return "202308310000"


# # def filter_bond_df_by_LUM(RGBbonding_df, RunningFileSHEET_ID, RunningFileCT, PreviousTime):   
# #     """
# #     Have to process bonding dataframe that time need to greater than previous LUM time but smaller
# #     the file"s create time.
    
# #     Because between two bonding time, only have once LUM inspection, so the the bonding dataframe need to 
# #     greater than previous LUM time if we have duplicate sheets ID.
# #     """        

# #     BondSheetDF = RGBbonding_df[RGBbonding_df["SHEET_ID"]==RunningFileSHEET_ID]
    
# #     if len(BondSheetDF.index) == 0:
# #         logging.info(f'[INFO] SHEET_ID "{str(RunningFileSHEET_ID)}" and bonding data are mismatch.')
        
# #     need2processDF = BondSheetDF[(BondSheetDF["CreateTime"] > PreviousTime) & (BondSheetDF["CreateTime"] < RunningFileCT)]
# #     del BondSheetDF
    
# #     return need2processDF    


# # def Bond_main_procedure(intersectionDataList):
# #     refpp = RepairFilePreprocess()
# #     fs = getGridFS(DBname="MT", collection_name="BondSummaryTable")
    
# #     # Process major data list
# #     for needProcessData in intersectionDataList:
# #         file_startswith = needProcessData.split("/")[-1].split("_")[1]
        
# #         if len(file_startswith) <= 4:
# #             continue
        
# #         else:
# #             majorData_df = refpp.Bond_df(needProcessData)
            
# #             AreaNO = refpp.getTargetAreaNo(majorData_df)
# #             CreateTime = refpp.getCT(majorData_df)
# #             OPID = refpp.getOPID(majorData_df)
# #             ToolID = refpp.getToolID(majorData_df)
# #             ModelNo = refpp.getModelNo(majorData_df)
# #             ABBR_No = refpp.getABBR_No(majorData_df)
# #             EQP_RecipeID = refpp.getEQP_Recipe_ID(majorData_df)
# #             SheetID = refpp.getSheetID(majorData_df)
# #             Source_CarrierID = refpp.getSource_CarrierID(majorData_df)
# #             RareaAssign = refpp.areaMatrix(majorData_df)
# #             GareaAssign = refpp.areaMatrix(majorData_df)
# #             BareaAssign = refpp.areaMatrix(majorData_df)
# #             logon, logoff = refpp.getLogOnOff(majorData_df)

# #             # Common columns
# #             common_value = [ToolID, SheetID, ModelNo, ABBR_No, OPID, EQP_RecipeID, Source_CarrierID, CreateTime, AreaNO, logon, logoff]
# #             R_df, G_df, B_df = refpp.RGB_df(majorData_df)
            
# #             R_data_list = common_value.copy()
# #             if len(R_df.index)!=0:
# #                 R = refpp.getLED_Type(R_df)
# #                 R_areaMatrix = refpp.coordinateToMatrix(R_df, RareaAssign)
# #                 R_bond_2D_id = fs.put(Binary(pickle.dumps(R_areaMatrix, protocol=5)))
# #                 R_Process, R_no_Process = refpp.areaQualityCount(R_areaMatrix)
# #                 R_data_list.extend([R, R_Process, R_no_Process, R_bond_2D_id])
# #             else:
# #                 R_data_list.extend(["R", "", "", ""])
                
# #             G_data_list = common_value.copy()
# #             if len(G_df.index)!=0:
# #                 G = refpp.getLED_Type(G_df)
# #                 G_areaMatrix = refpp.coordinateToMatrix(G_df, GareaAssign)
# #                 G_bond_2D_id = fs.put(Binary(pickle.dumps(G_areaMatrix, protocol=5)))
# #                 G_Process, G_No_Process = refpp.areaQualityCount(G_areaMatrix)
# #                 G_data_list.extend([G, G_Process, G_No_Process, G_bond_2D_id])
# #             else:
# #                 G_data_list.extend(["G", "", "", ""])
            
# #             B_data_list = common_value.copy()
# #             if len(B_df.index)!=0:
# #                 B = refpp.getLED_Type(B_df)
# #                 B_areaMatrix = refpp.coordinateToMatrix(B_df, BareaAssign)    
# #                 B_bond_2D_id = fs.put(Binary(pickle.dumps(B_areaMatrix, protocol=5)))
# #                 B_Process, B_no_Process = refpp.areaQualityCount(B_areaMatrix)
# #                 B_data_list.extend([B, B_Process, B_no_Process, B_bond_2D_id])
# #             else:
# #                 B_data_list.extend(["B", "", "", ""])

# #             CreateBondSummary(DBname="MT", collection_name="BondSummaryTable", R_data_list=R_data_list, G_data_list=G_data_list, B_data_list=B_data_list)
        



