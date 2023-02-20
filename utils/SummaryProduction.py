import pandas as pd
import numpy as np
import os
from utils._config_ import RS, GS, BS, file_path
from utils._config_ import T161FUN01, T136FUN01, T173XUN01, V130FLN02
import numpy.typing as npt
import logging
from utils._BondingSummaryProduction_ import bonding_processing
from utils.sendMail import alarmAutoMail, customMessageAutoMail


class Summary_produce(bonding_processing):
    def __init__(self):
        super().__init__()
        self.RS = RS
        self.GS = GS
        self.BS = BS
        self.summary_table_save_path = file_path['Summary_table_save_path']
        self.fullBondSummaryTableSP = file_path['FullBondSummary_table_save_path']

    def check_model_no(self, file_list:list):
        checked_file_list = []
        print("[INFO] Checking File's MODEL_NO and Size...")

        # sftp_client = self.connectSSH()
        try:
            for file in file_list:
                df = pd.read_csv(file, sep='dialect', engine='python')
                new_df = df['_FACTOR'].str.split(',', expand=True)
                w, h = self.reshape_following_data(new_df[new_df[6]=='R'].reset_index(drop=True))

                MODEL_NO = new_df.iat[3,0].split("=")[1].split("_")[0]
                logging.warning(f"[INFO] Checking {file} MODEL NO : {MODEL_NO}")
                # 轉90度
                if MODEL_NO == self.MASK_key[3]:
                    if w != (V130FLN02["COC2_Y_PIXEL"]*V130FLN02["SW_Y_COC2"]) or h != (V130FLN02["COC2_X_PIXEL"]*V130FLN02["SW_X_COC2"]):
                        logging.warning(
                            f"[INFO] File '{file}' MODEL_NO and PRODUCT SIZE are not match.\n"
                            f"The file width & height is {w}, {h}."
                        )
                        continue
                    else:
                        checked_file_list.append(file)
                # 沒轉90度
                elif MODEL_NO == self.MASK_key[0] or MODEL_NO == 'T161XUN01.0':
                    if w != (T161FUN01["COC2_X_PIXEL"]*T161FUN01["SW_X_COC2"])  or h != (T161FUN01["COC2_Y_PIXEL"]*T161FUN01["SW_Y_COC2"]):
                        logging.warning(
                            f"[INFO] File '{file}' MODEL_NO and PRODUCT SIZE are not match.\n"
                            f"The file width & height is {w}, {h}.\n"
                        )                    
                        continue
                    else:
                        checked_file_list.append(file)
                # 轉90度
                elif MODEL_NO == self.MASK_key[1]:
                    if w != (T136FUN01["COC2_Y_PIXEL"]*T136FUN01["SW_Y_COC2"]) or h != (T136FUN01["COC2_X_PIXEL"]*T136FUN01["SW_X_COC2"]):
                        logging.warning(
                            f"[INFO] File '{file}' MODEL_NO and PRODUCT SIZE are not match.\n"
                            f"The file width & height is {w}, {h}.\n"
                        )                  
                        continue
                    else:
                        checked_file_list.append(file)
                # 轉90度
                elif MODEL_NO == self.MASK_key[2]:
                    if w != (T173XUN01["COC2_Y_PIXEL"]*T173XUN01["SW_Y_COC2"]) or h != (T173XUN01["COC2_X_PIXEL"]*T173XUN01["SW_X_COC2"]):
                        logging.warning(
                            f"[INFO] File '{file}' MODEL_NO and PRODUCT SIZE are not match.\n"
                            f"The file width & height is {w}, {h}.\n"
                        )                   
                        continue
                    else:
                        checked_file_list.append(file)
                else:
                    logging.error(f"[Warning] MODEL_NO '{MODEL_NO}' was not in PRODUCT.json")
                    alarmAutoMail().send(file, MODEL_NO)
                    continue

        except Exception as E:
            logging.error(str(E))

        return checked_file_list


    def AOI_TimeRange(self, checked_file_list):
        print("[INFO] Creat AOI Time Summary...")
        df_list = []
        # sftp_client = self.connectSSH()
        for ckfile in checked_file_list:
            df = pd.read_csv(ckfile, sep='dialect', engine='python')
            df = df['_FACTOR'].str.split(',', expand=True)
            new_df_tmp = df[df[6]=='R'].copy()
            new_df_tmp["SheetID"] = str(df.iat[2,0].split("=")[1])
            df_group = new_df_tmp.groupby([0])[["SheetID"]].first().reset_index()
            df_group.rename(columns = {0:'CreateTime'}, inplace = True)
            df_list.append(df_group)
        try:
            rangeFile = pd.concat(df_list)
            pd.DataFrame(rangeFile).to_csv("./AOI_Summary.csv", mode='w')    
        except Exception as E:
            logging.error(str(E))
        print("[INFO] AOI_Summary.csv has created.")


    def findPreviousTime(self, AOI_TimeRange_df, RunningFileSHEET_ID, RunningFileCT):
        if "-" in RunningFileSHEET_ID:
            newRunningFileSHEET_ID = RunningFileSHEET_ID.split("-")[0]
            AOISheetDF = AOI_TimeRange_df.loc[AOI_TimeRange_df["SheetID"].str[:len(newRunningFileSHEET_ID)] == newRunningFileSHEET_ID].reset_index(drop=True)

        if "-" not in RunningFileSHEET_ID:
            AOISheetDF = AOI_TimeRange_df.loc[AOI_TimeRange_df["SheetID"] == RunningFileSHEET_ID].reset_index(drop=True)
        realTimeLi = sorted(AOISheetDF["CreateTime"].tolist())

        for previous, current in zip(realTimeLi, realTimeLi[1:]):
            if str(current) == RunningFileCT:
                return str(previous)
        return "202210010000"
            
        
    def reshape_following_data(self, R_df):
        width, height = R_df[8].astype('int').max(), R_df[10].astype('int').max()
        return width, height


    def process_row_data(self, file_path):
        FILE_name = file_path.split('/')[-1].split('.')[0]
        # sftp_client = self.connectSSH()
        df = pd.read_csv(file_path, sep='dialect', engine='python', encoding="utf-8-sig")
        new_df = df['_FACTOR'].str.split(',', expand=True)
        return df, new_df, FILE_name


    def RGB_df_FromRowData(self, new_df):
        R_df = new_df[new_df[6]=='R'].reset_index(drop=True)
        G_df = new_df[new_df[6]=='G'].reset_index(drop=True)
        B_df = new_df[new_df[6]=='B'].reset_index(drop=True)
        return R_df, G_df, B_df


    def assignCommonValue(self, df):
        # append recipe & opid
        columnlist = []
        init_len = [i.split('=')[-1] for i in df['_FACTOR'].tolist()[0:5]]
        datalist = [i.split('=')[-1] for i in df['_FACTOR'].tolist()]

        for i in datalist:
            if i not in init_len and i != '_DATA':
                logging.warning(f"[INFO] {i} was append to New column") 
            if i != '_DATA':
                columnlist.append(i)
            else:
                break
        return columnlist


    def checkColumnList(self, columnlist):
        SHEET_ID, MODEL_NO,  ABBR_NO, EQP_Recipe_ID, OPID = 0, 0, 0, 0, 0

        if len(columnlist) <= 5:
            SHEET_ID, MODEL_NO,  ABBR_NO = columnlist[2:5]
            MODEL_NO = MODEL_NO.split("_")[0]
            EQP_Recipe_ID = 0
            OPID = 0
        elif 7 >= len(columnlist) >= 5 :
            SHEET_ID, MODEL_NO,  ABBR_NO = columnlist[2:5]
            MODEL_NO = MODEL_NO.split("_")[0]
            EQP_Recipe_ID, OPID = columnlist[5:7]
        else:
            logging.error(f"Column Name {columnlist[7:]} cannot identify.")
            message = f'Column Name {columnlist[7:]} cannot identify.'
            customMessageAutoMail().send(message)
        return SHEET_ID,  MODEL_NO, ABBR_NO, EQP_Recipe_ID, OPID 


    def CreateSummaryTable(self, RGBdf, SheetID, Model_NO, ABBR_NO, OPID, EQP_Recipe_ID):
        new_df_tmp = RGBdf.copy()

        new_df_tmp['NGCNT'] = np.where((new_df_tmp[11]=='0'), 1, 0)
        new_df_tmp['OKCNT'] = np.where((new_df_tmp[11]=='1'), 1, 0)
        df_group=new_df_tmp.groupby([0, 1, 2, 6, 12])[['NGCNT', 'OKCNT']].agg(sum).reset_index()

        df_group.insert(loc=1, column='SHEET_ID', value=str(SheetID))
        df_group.insert(loc=3, column='Model_NO', value=Model_NO)
        df_group.insert(loc=4, column='ABBR_NO', value=ABBR_NO)
        df_group.insert(loc=5, column='EQP_Recipe_ID', value=EQP_Recipe_ID)
        df_group.insert(loc=6, column='OPID', value=OPID)

        df_group.rename(columns = {0:'CreateTime', 1:'MES_ID', 2:'TOOL_ID', 6:'LED_TYPE', 12:'Defect_Code'}, inplace = True)

        key_list = ['CreateTime', 'SHEET_ID', 'MES_ID', 'TOOL_ID', 'Model_NO', 'ABBR_NO', 'EQP_Recipe_ID', 'OPID', 'LED_TYPE', 'Defect_Code', 'NGCNT', 'OKCNT']
        df_group_tmp = df_group[key_list]

        df_group_tmp = df_group_tmp.copy()
        df_group_tmp['TOTALCNT']=df_group_tmp['OKCNT'].sum() + df_group_tmp['NGCNT'].sum()

        return df_group_tmp


    def assign_col(self, df, LightingCheck_2D, DefectCode_2D, Luminance_2D, YiledAnalysis_2D, 
        Process_OK, Process_NG, NO_Process_OK, NO_Process_NG, BSR, LTR):
        new_df = df.copy()
        new_df = new_df.assign(
            HeatMap=0,
            LightingCheck_2D = LightingCheck_2D,
            DefectCode_2D = DefectCode_2D,
            Luminance_2D = Luminance_2D,
            YiledAnalysis_2D = YiledAnalysis_2D, 
            Process_OK = Process_OK,
            Process_NG = Process_NG,
            NO_Process_OK = NO_Process_OK,
            NO_Process_NG = NO_Process_NG,
            Bond_Success_Rate = BSR,
            Lighting_Rate = LTR
        )
        return new_df



    def rgbTypeLightingCheck(self, R_df, G_df, B_df):
        w, h = self.reshape_following_data(R_df)

        count_R = np.asarray(R_df[11][:], dtype='uint8')
        count_G = np.asarray(G_df[11][:], dtype='uint8')
        count_B = np.asarray(B_df[11][:], dtype='uint8')
        RlightingArray = np.flip(np.flip(count_R.reshape((h, w)), 0), 1)
        GlightingArray = np.flip(np.flip(count_G.reshape((h, w)), 0), 1)
        BlightingArray = np.flip(np.flip(count_B.reshape((h, w)), 0), 1)
        return RlightingArray, GlightingArray, BlightingArray


    def reshapeLightingArray(self, new_df, lightingArray: npt.ArrayLike):
        MODEL_NO = new_df.iat[3,0].split("=")[1].split("_")[0]
        try:
            if MODEL_NO == self.MASK_key[0] or MODEL_NO =='T161XUN01.0': 
                reshpeLightingArray = np.reshape(
                    lightingArray, 
                    (T161FUN01["SW_Y_COC2"], T161FUN01["SW_X_COC2"], T161FUN01["COC2_Y_PIXEL"], T161FUN01["COC2_X_PIXEL"])
                )
            elif MODEL_NO == self.MASK_key[1]:
                reshpeLightingArray = np.reshape(
                    lightingArray, 
                    (T136FUN01["SW_X_COC2"], T136FUN01["SW_Y_COC2"], T136FUN01["COC2_X_PIXEL"], T136FUN01["COC2_Y_PIXEL"])
                )
            elif MODEL_NO == self.MASK_key[2]:
                reshpeLightingArray = np.reshape(
                    lightingArray, 
                    (T173XUN01["SW_X_COC2"], T173XUN01["SW_Y_COC2"], T173XUN01["COC2_X_PIXEL"], T173XUN01["COC2_Y_PIXEL"])
                )
            elif MODEL_NO == self.MASK_key[3]:
                reshpeLightingArray = np.reshape(
                    lightingArray, 
                    (V130FLN02["SW_X_COC2"], V130FLN02["SW_Y_COC2"], V130FLN02["COC2_X_PIXEL"], V130FLN02["COC2_Y_PIXEL"])
                )
        except NameError as NE:
            logging.error(NE(f"[Error] MODEL_NO {MODEL_NO} did not in MASK key."))
        return reshpeLightingArray


    


    def BulidUpFullBondingMatrix(self, SHEET_ID: str, RGBbonding_df: pd.DataFrame, reshpeLightingArray: npt.ArrayLike, 
                                 LED_Type: str, MODEL_NO: str):
        AOI_SHEET_ID = SHEET_ID
        try:    
            if "-" in AOI_SHEET_ID:
                UpdateAOI_SHEET_ID = SHEET_ID.split("-")[0]
                sheetID_df = RGBbonding_df[RGBbonding_df["SHEET_ID"]==UpdateAOI_SHEET_ID]
                
            else:
                sheetID_df = RGBbonding_df[RGBbonding_df["SHEET_ID"]==AOI_SHEET_ID]  

            by_led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
            bonding_2D_path = by_led_type_df["Bonding_Matrix"].tolist()
            Target_Area_No = by_led_type_df["Target_Area_No"].tolist()
            zeros_matrix = np.zeros_like(reshpeLightingArray)

            for b2Dp, area in zip(bonding_2D_path, Target_Area_No):
                b2D = np.asarray(pd.read_csv(b2Dp), dtype='uint8')
                if MODEL_NO == self.MASK_key[1] or MODEL_NO == self.MASK_key[3]:
                    dict136 = {
                        'A':[0,0],
                        'B':[0,1],
                        'C':[0,2],
                        'D':[1,0],
                        'E':[1,1],
                        'F':[1,2]
                    }
                    zeros_matrix[dict136.get(area)[0]][dict136.get(area)[1]]=b2D

                elif MODEL_NO == self.MASK_key[0] or MODEL_NO =='T161XUN01.0':
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
                    zeros_matrix[dict161.get(area)[0]][dict161.get(area)[1]]=b2D
                else:
                    raise NameError
  
            fully_bond_matrix = zeros_matrix

        except NameError(f"SheetID {str(AOI_SHEET_ID)} can not be identified"):
            logging.error(NameError)
            raise
        return fully_bond_matrix


    def BulidUpAreaBondingMatrix(self, new_df: pd.DataFrame, need2processDF: pd.DataFrame, reshpeLightingArray: npt.
                                 ArrayLike, LED_Type: str, fileName: str, MODEL_NO: str):
        AOI_SHEET_ID = new_df.iat[2,0].split("=")[1]
        AOI_SHEET_ID = str(AOI_SHEET_ID)
        AOICT = new_df.iat[15,0]
        try:    
            if "-" in AOI_SHEET_ID:
                UpdateAOI_SHEET_ID = new_df.iat[2,0].split("=")[1].split("-")[0]
                reTestTime = new_df.iat[2,0].split("=")[1].split("-")[1]
                sheetID_df = need2processDF[need2processDF["SHEET_ID"]==UpdateAOI_SHEET_ID]
                by_led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
                by_led_type_df.insert(loc=2, column="Re_Test", value=reTestTime)
                
            else:
                sheetID_df = need2processDF[need2processDF["SHEET_ID"]==AOI_SHEET_ID]  
                by_led_type_df = sheetID_df[sheetID_df["LED_TYPE"]==LED_Type]
                by_led_type_df.insert(loc=2, column="Re_Test", value=0)

            by_led_type_df.insert(loc=9, column="AOI_CreateTime", value=AOICT)
            bonding_2D_path = by_led_type_df["Bonding_Matrix"].tolist()
            Target_Area_No = by_led_type_df["Target_Area_No"].tolist()

            pok, png, nok, npg = [], [], [], []
            Yelid_path_list = []
            for b2Dp, area in zip(bonding_2D_path, Target_Area_No):
                b2D = np.asarray(pd.read_csv(b2Dp), dtype='uint8')
                if MODEL_NO == self.MASK_key[1] or MODEL_NO == self.MASK_key[3]:
                    dict136 = {
                        'A':[0,0],
                        'B':[0,1],
                        'C':[0,2],
                        'D':[1,0],
                        'E':[1,1],
                        'F':[1,2]
                    }
                    zeros_matrix = b2D + reshpeLightingArray[dict136.get(area)[0]][dict136.get(area)[1]]
                    sp = self.identityLedSaveB2D(LED_Type, zeros_matrix, area, fileName)
                    Aprocess_ok, Aprocess_ng, Ano_process_ok, Ano_process_ng = self.AreaYieldCheck(zeros_matrix)
                    pok.extend([Aprocess_ok])
                    png.extend([Aprocess_ng])
                    nok.extend([Ano_process_ok])
                    npg.extend([Ano_process_ng])
                    Yelid_path_list.extend([sp])
                

                elif MODEL_NO == self.MASK_key[0] or MODEL_NO =='T161XUN01.0':
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
                    zeros_matrix = b2D + reshpeLightingArray[dict161.get(area)[0]][dict161.get(area)[1]]
                    sp = self.identityLedSaveB2D(LED_Type, zeros_matrix, area, fileName)
                    Aprocess_ok, Aprocess_ng, Ano_process_ok, Ano_process_ng = self.AreaYieldCheck(zeros_matrix)
                    pok.extend([Aprocess_ok])
                    png.extend([Aprocess_ng])
                    nok.extend([Ano_process_ok])
                    npg.extend([Ano_process_ng])
                    Yelid_path_list.extend([sp])
                    
                else:
                    logging.warning(f"MODEL_NO '{MODEL_NO}' can not be identified")
            
            LED_TYPE_df = by_led_type_df.copy()
            LED_TYPE_df = LED_TYPE_df.assign(
                Process_ok = pok,
                Process_NG = png,
                NO_Process_OK = nok,
                No_Process_NG = npg,
                AreaYield_2D_SavePath = Yelid_path_list)

        except NameError as e:
            logging.error(e(f"[Error] SheetID '{str(AOI_SHEET_ID)}' can not be identified"))
        except Exception:
            logging.error(str(Exception))
        return LED_TYPE_df


    def filterBondDataByAOI(self, RGBbonding_df, RunningFileSHEET_ID, RunningFileCT, PreviousTime):           
        # initDate = '202210010000'
        if "-" in RunningFileSHEET_ID:
            RunningFileSHEET_ID = RunningFileSHEET_ID.split("-")[0]

        BondSheetDF = RGBbonding_df[RGBbonding_df["SHEET_ID"]==RunningFileSHEET_ID]
        if len(BondSheetDF.index) == 0:
            logging.error(f"[INFO] SHEET_ID '{str(RunningFileSHEET_ID)}' and bonding data are mismatch.")

        need2processDF = BondSheetDF[(BondSheetDF["CreateTime"]> PreviousTime) & (BondSheetDF["CreateTime"] < RunningFileCT)]
        # if len(need2processDF.index) == 0:
        #     need2processDF = BondSheetDF[(BondSheetDF["CreateTime"] > initDate) & (BondSheetDF["CreateTime"] < RunningFileCT)]
        #     return need2processDF

        return need2processDF


    def AreaYieldCheck(self, matrixList: npt.ArrayLike):
        process_ok = np.count_nonzero(matrixList==11)
        process_ng = np.count_nonzero(matrixList==10)
        no_process_ok = np.count_nonzero(matrixList==1)
        no_process_ng = np.count_nonzero(matrixList==0)

        return process_ok, process_ng, no_process_ok, no_process_ng


    def identityLedSaveB2D(self, led_type: str, matrix :npt.ArrayLike, area:str, fileName:str):
        R_bond_2D_sp = self.Bonding_R_2D_sp + "R_" + area + "_" + fileName + self.zip_fileType
        G_bond_2D_sp = self.Bonding_G_2D_sp + "G_" + area + "_" + fileName + self.zip_fileType
        B_bond_2D_sp = self.Bonding_B_2D_sp + "B_" + area + "_" + fileName + self.zip_fileType

        if led_type == 'R':
            savePath = R_bond_2D_sp
            pd.DataFrame(matrix).to_csv(savePath, index=False, compression= 'zip')
        elif led_type == 'G':
            savePath = G_bond_2D_sp
            pd.DataFrame(matrix).to_csv(savePath, index=False, compression='zip')
        else:
            savePath = B_bond_2D_sp
            pd.DataFrame(matrix).to_csv(savePath, index=False, compression='zip')    
        return savePath


    def concatRGBdf(self, R_BOND_DF, G_BOND_DF, B_BOND_DF):
        SheetID_df = pd.concat([R_BOND_DF, G_BOND_DF, B_BOND_DF])
        return SheetID_df


    def fullYieldCheck(self, fully_bond_matrix: npt.ArrayLike, reshpeLightingArray: npt.ArrayLike, OPID):
        res = fully_bond_matrix + reshpeLightingArray
        
        process_ok = np.count_nonzero(res==11)
        process_ng = np.count_nonzero(res==10)
        no_process_ok = np.count_nonzero(res==1)
        no_process_ng = np.count_nonzero(res==0)

        if OPID == 'TNLBO' or OPID == 'UM-BON':
            process_ok = no_process_ok
            process_ng = no_process_ng
            no_process_ok = 0
            no_process_ng = 0

        totalBond = process_ok + process_ng
        total = no_process_ok + no_process_ng + process_ok + process_ng
        lighting = process_ok + no_process_ok

        if totalBond == 0:
            BSR = 100
        else:
            BSR = np.round((process_ok/totalBond)*100, 3)
            
        LTR = np.round((lighting/total)*100, 3)
        
        return res, process_ok, process_ng, no_process_ok, no_process_ng, BSR, LTR


    def file2CSV(self, saveFolder, LED_LC_Array:npt.ArrayLike, defect_code_2D:npt.ArrayLike, LUM2D:npt.ArrayLike, file_name:str):
        LCsp = saveFolder + "Lighting_Check_" + file_name + self.zip_fileType
        DCsp = saveFolder + "Defect_Code_" + file_name + self.zip_fileType
        LUMsp = saveFolder + "LUM2D_" + file_name + self.zip_fileType
        pd.DataFrame(LED_LC_Array).to_csv(LCsp, index=False, compression='zip')
        pd.DataFrame(defect_code_2D).to_csv(DCsp, index=False, compression='zip')
        pd.DataFrame(LUM2D).to_csv(LUMsp, index=False, compression='zip')
        return LCsp, DCsp, LUMsp


    def Yield2csv(self, R_df, Rres:np.array, Gres:np.array, Bres:np.array, file_name):
        w, h = self.reshape_following_data(R_df)

        RY = self.RS + "R_Yield_" + file_name + self.zip_fileType
        GY = self.GS + "G_Yield_" + file_name + self.zip_fileType
        BY = self.BS + "B_Yield_" + file_name + self.zip_fileType

        RresReshapeTo2D = Rres.reshape((h, w))
        GresReshapeTo2D = Gres.reshape((h, w))
        BresReshapeTo2D = Bres.reshape((h, w))
        pd.DataFrame(RresReshapeTo2D).to_csv(RY, index=False, compression='zip')
        pd.DataFrame(GresReshapeTo2D).to_csv(GY, index=False, compression='zip')
        pd.DataFrame(BresReshapeTo2D).to_csv(BY, index=False, compression='zip')
        return RY, GY, BY


    def Luminance_2D(self, R_df, G_df, B_df):
        w, h = self.reshape_following_data(R_df)
        R_lum = np.asarray(R_df[13][:])
        G_lum = np.asarray(G_df[13][:])
        B_lum = np.asarray(B_df[13][:])

        R_lum_2d = np.flip(np.flip(R_lum.reshape((h, w)), 0), 1)
        G_lum_2d = np.flip(np.flip(G_lum.reshape((h, w)), 0), 1)
        B_lum_2d = np.flip(np.flip(B_lum.reshape((h, w)), 0), 1)
        return R_lum_2d, G_lum_2d, B_lum_2d


    def defect_code_2D(self, R_df, G_df, B_df):
        w, h = self.reshape_following_data(R_df)
        R_DC = np.asarray(R_df[12][:])
        G_DC = np.asarray(G_df[12][:])
        B_DC = np.asarray(B_df[12][:])

        R_DEFECT_CODE_2D = np.flip(np.flip(R_DC.reshape((h, w)), 0), 1)
        G_DEFECT_CODE_2D = np.flip(np.flip(G_DC.reshape((h, w)), 0), 1)
        B_DEFECT_CODE_2D = np.flip(np.flip(B_DC.reshape((h, w)), 0), 1)
        return R_DEFECT_CODE_2D, G_DEFECT_CODE_2D, B_DEFECT_CODE_2D


    def summary_table_production(self, Rdf, Gdf, Bdf):
        whole_df = pd.concat([Rdf, Gdf, Bdf])
        self.save_summary_file(whole_df, self.summary_table_save_path)
        return whole_df


    def save_summary_file(self, whole_df: pd.DataFrame, filePath:str):
        if os.path.exists(filePath):
            pd.DataFrame(whole_df).to_csv(
                path_or_buf=filePath, 
                mode='a', 
                index=False, 
                encoding="utf_8_sig",
                header=False
            )
        else:
            pd.DataFrame(whole_df).to_csv(
                path_or_buf=filePath, 
                mode='w', 
                index=False, 
                encoding="utf_8_sig"
            )    








    


        

        

        
