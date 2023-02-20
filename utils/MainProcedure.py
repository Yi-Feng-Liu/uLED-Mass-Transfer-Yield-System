import logging
from utils._AOISummaryProduction_ import Summary_produce
from utils._BondingSummaryProduction_ import bonding_processing, bond_summary
import pandas as pd
from utils._config_ import file_path


def AOI_main_procedure(AOIfile_path):
    sp = Summary_produce()
    bonding_df = pd.read_csv(file_path["BondSummary_table_save_path"], dtype=str)
    AOI_TimeRange_df = pd.read_csv("./AOI_Summary.csv", dtype=str)

    df, new_df, FILE_name = sp.process_row_data(AOIfile_path)
    RunFileCT = new_df.iat[15,0]

    R_bond_df = bonding_df[bonding_df["LED_TYPE"]=="R"]
    G_bond_df = bonding_df[bonding_df["LED_TYPE"]=="G"]
    B_bond_df = bonding_df[bonding_df["LED_TYPE"]=="B"]

    R_df, G_df, B_df = sp.RGB_df_FromRowData(new_df)
    common_value = sp.assignCommonValue(df)

    SHEET_ID, MODEL_NO, ABBR_NO, EQP_Recipe_ID, OPID  = sp.checkColumnList(common_value)
    logging.warning(f"Running {SHEET_ID}")

    previousTime = sp.findPreviousTime(AOI_TimeRange_df, SHEET_ID, RunFileCT)

    R_summary = sp.CreateSummaryTable(R_df, SHEET_ID, MODEL_NO, ABBR_NO, OPID, EQP_Recipe_ID)
    G_summary = sp.CreateSummaryTable(G_df, SHEET_ID, MODEL_NO, ABBR_NO, OPID, EQP_Recipe_ID)
    B_summary = sp.CreateSummaryTable(B_df, SHEET_ID, MODEL_NO, ABBR_NO, OPID, EQP_Recipe_ID)

    RDC2D, GDC2D, BDC2D= sp.defect_code_2D(R_df, G_df, B_df)

    R2DArray, G2DArray, B2DArray = sp.rgbTypeLightingCheck(R_df, G_df, B_df)

    R_reshape = sp.reshapeLightingArray(new_df, R2DArray)
    G_reshape = sp.reshapeLightingArray(new_df, G2DArray)
    B_reshape = sp.reshapeLightingArray(new_df, B2DArray)

    R_need2process = sp.filterBondDataByAOI(R_bond_df, SHEET_ID, RunFileCT, previousTime)
    G_need2process = sp.filterBondDataByAOI(G_bond_df, SHEET_ID, RunFileCT, previousTime)
    B_need2process = sp.filterBondDataByAOI(B_bond_df, SHEET_ID, RunFileCT, previousTime)

    R_BondFullyMatrix = sp.BulidUpFullBondingMatrix(SHEET_ID, R_need2process, R_reshape, 'R', MODEL_NO)
    G_BondFullyMatrix = sp.BulidUpFullBondingMatrix(SHEET_ID, G_need2process, G_reshape, 'G', MODEL_NO)
    B_BondFullyMatrix = sp.BulidUpFullBondingMatrix(SHEET_ID, B_need2process, B_reshape, 'B', MODEL_NO)

    R_res, RPOK, RPNG, RNPOK, RNPNG, RBSR, RLTR = sp.fullYieldCheck(R_BondFullyMatrix, R_reshape, OPID)
    G_res, GPOK, GPNG, GNPOK, GNPNG, GBSR, GLTR = sp.fullYieldCheck(G_BondFullyMatrix, G_reshape, OPID)
    B_res, BPOK, BPNG, BNPOK, BNPNG, BBSR, BLTR = sp.fullYieldCheck(B_BondFullyMatrix, B_reshape, OPID)
    RY, GY, BY = sp.Yield2csv(R_df, R_res, G_res, B_res, FILE_name)
    
    R_BOND_DF = sp.BulidUpAreaBondingMatrix(new_df, R_need2process, R_reshape, 'R', FILE_name, MODEL_NO)
    G_BOND_DF = sp.BulidUpAreaBondingMatrix(new_df, G_need2process, G_reshape, 'G', FILE_name, MODEL_NO)
    B_BOND_DF = sp.BulidUpAreaBondingMatrix(new_df, B_need2process, B_reshape, 'B', FILE_name, MODEL_NO)

    AOI_CorresBond_SheetID_df = sp.concatRGBdf(R_BOND_DF, G_BOND_DF, B_BOND_DF)
    
    R_lum2D, G_lum2D, B_lum2D = sp.Luminance_2D(R_df, G_df, B_df)
    
    RLCsp, RDCsp, RLUMsp = sp.file2CSV(sp.RS, R2DArray, RDC2D, R_lum2D, FILE_name)
    GLCsp, GDCsp, GLUMsp = sp.file2CSV(sp.GS, G2DArray, GDC2D, G_lum2D, FILE_name)
    BLCsp, BDCsp, BLUMsp = sp.file2CSV(sp.BS, B2DArray, BDC2D, B_lum2D, FILE_name)

    R_summary = sp.assign_col(R_summary, RLCsp, RDCsp, RLUMsp, RY, RPOK, RPNG, RNPOK, RNPNG, RBSR, RLTR)
    G_summary = sp.assign_col(G_summary, GLCsp, GDCsp, GLUMsp, GY, GPOK, GPNG, GNPOK, GNPNG, GBSR, GLTR)
    B_summary = sp.assign_col(B_summary, BLCsp, BDCsp, BLUMsp, BY, BPOK, BPNG, BNPOK, BNPNG, BBSR, BLTR)

    sp.summary_table_production(R_summary, G_summary, B_summary)
    
    return AOI_CorresBond_SheetID_df
    

def Bond_main_procedure(intersectionDataList):
    bp = bonding_processing()
    bs = bond_summary()

    # Process major data list
    for needProcessData in intersectionDataList:

        majorData_df, fileName = bp.Bond_df(needProcessData)
        
        AreaNO = bp.assignTargetAreaNo(majorData_df)
        CreateTime = bp.assignCT(majorData_df)
        OPID = bp.assignOPID(majorData_df)
        ToolID = bp.assignToolID(majorData_df)
        ModelNo = bp.assignModelNo(majorData_df)
        ABBR_No = bp.assignABBR_No(majorData_df)
        EQP_RecipeID = bp.assignEQP_Recipe_ID(majorData_df)
        SheetID = bp.assignSheetID(majorData_df)
        Source_CarrierID = bp.assignSource_CarrierID(majorData_df)
        RareaAssign = bp.areaMatrix(majorData_df)
        GareaAssign = bp.areaMatrix(majorData_df)
        BareaAssign = bp.areaMatrix(majorData_df)
        logon, logoff = bp.assignLogOnOff(majorData_df)

        # Common value
        common_value = [ToolID, SheetID, ModelNo, ABBR_No, OPID, EQP_RecipeID, Source_CarrierID, CreateTime, AreaNO, logon, logoff]
        R_data_list = common_value.copy()
        G_data_list = common_value.copy()
        B_data_list = common_value.copy()

        R_df, G_df, B_df = bp.RGB_df(majorData_df)

        R = bp.assignLED_Type(R_df)
        R_bond_2D_sp = bp.Bonding_R_2D_sp + "R_" + fileName + bp.zip_fileType
        R_areaMatrix = bp.coordinateToMatrix(R_df, RareaAssign)
        R_Process, R_no_Process = bp.areaQualityCount(R_areaMatrix)

        pd.DataFrame(R_areaMatrix).to_csv(R_bond_2D_sp, index=False, compression='zip')
        R_data_list.extend([R, R_Process, R_no_Process, R_bond_2D_sp])

        G = bp.assignLED_Type(G_df)
        G_bond_2D_sp = bp.Bonding_G_2D_sp + "G_" + fileName + bp.zip_fileType
        G_areaMatrix = bp.coordinateToMatrix(G_df, GareaAssign)
        G_Process, G_No_Process = bp.areaQualityCount(G_areaMatrix)
        pd.DataFrame(G_areaMatrix).to_csv(G_bond_2D_sp, index=False, compression='zip')
        G_data_list.extend([G, G_Process, G_No_Process, G_bond_2D_sp])

        B = bp.assignLED_Type(B_df)
        B_bond_2D_sp = bp.Bonding_B_2D_sp + "B_" + fileName + bp.zip_fileType
        B_areaMatrix = bp.coordinateToMatrix(B_df, BareaAssign)                                        
        B_Process, B_no_Process = bp.areaQualityCount(B_areaMatrix)
        pd.DataFrame(B_areaMatrix).to_csv(B_bond_2D_sp, index=False, compression='zip')
        B_data_list.extend([B, B_Process, B_no_Process, B_bond_2D_sp])

        whole_df = bs.createDataFrame(R_data_list, G_data_list, B_data_list)
        bs.save_summary_file(whole_df)



