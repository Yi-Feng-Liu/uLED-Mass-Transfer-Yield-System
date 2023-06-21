import matplotlib 
matplotlib.use('agg') 
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from datetime import timedelta
import schedule
import logging
import os
from utils._CreatePPt_ import PPTmain
import shutil
from utils.sendMail import auto_mail, customMessageAutoMail
import time
import numpy as np
from SFTP.Connection import getLightOnResult
import math
from bson import ObjectId
import pickle
from pymongo import MongoClient
import numpy.typing as npt
import gridfs
import math


class TableType():
    """Created table will be send by email.

    All of table type function in this class, including table by sheet, table by hours, table by day, table by week, table by month.
    """
    def __init__(self):
        self.client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017')
        db = self.client["MT"]
        self.collection = db["LUM_SummaryTable"]     
        self.fs = gridfs.GridFS(db, collection='LUM_SummaryTable')
        # 留下只想讀取的column
        self.key_list = ['CreateTime', 'OPID', 'Defect_Code', 'SHEET_ID', 'LED_TYPE', 'NGCNT', 'Yield', 'Grade', 'Process_OK', 'Process_NG', 'NO_Process_OK', 'NO_Process_NG', 'Bond_Success_Rate', 'LightingCheck_2D', 'Luminance_2D', 'YiledAnalysis_2D']
        self.fontsize = 14
        self.R_color = 'fuchsia'
        self.G_color = 'mediumseagreen'
        self.B_color = 'blue'
        self.logPath = './log/'
        self.reportImgPath = './report_production/'
        self.MainBondImgSavePath136 = self.reportImgPath + 'MainBondYiled13.6/'
        self.MainBondImgSavePath161 = self.reportImgPath + 'MainBondYiled16.1/'
        self.MainBondImgSavePath173 = self.reportImgPath + 'MainBondYiled17.3/'
        self.reportImgPath136 = self.reportImgPath + '13.6/'
        self.reportImgPath161 = self.reportImgPath + '16.1/'
        self.reportImgPath173 = self.reportImgPath + '17.1/'
        
        self.OPID_comparison_table = {
            'CNAPL':'CO-M-AOI Plasma',
            'CRAPL':'CO-R-AOI Plasma', 
            'TNAB2':'TFT-M-AOI Shiping',
            'TNABO':'TFT-M-AOI Bonding',
            'TNACL':'TFT-M-AOI Clean',
            'TNLAT':'TFT-M-light ATC',
            'TNLBO':'TFT-M-light Bonding',
            'TNLCL':'TFT-M-light CLN for Ship',
            'TNLLA':'TFT-M-light Laser cut',
            'TNLPL':'TFT-M-light Plasma',
            'TRADE':'TFT-R-AOI Debond',
            'TRARE':'TFT-R-AOI Repair-Bond',
            'TRLAG':'TFT-R-light Aging',
            'TRLLM':'TFT-R-light LSMT',
            'TRLRE':'TFT-R-light Repair-Bond',
            'TNOCL': 'TNOCL',
            'TROCL': 'TROCL',
            'UM-BON': 'UM-BON',
            'UM-CLN': 'UM-CLN',
            'UM-OVE': 'UM-OVE',
            'UM-STA': 'UM-STA',
            'SHIP' : 'SHIP',
            'FNSHIP': 'FNSHIP'
        }
        
        self.labels = ["R_Repair_NG", "R_No_Repair_but_NG", "R_Repair_OK", "G_Repair_NG", "G_No_Repair_but_NG", "G_Repair_OK", "B_Repair_NG", "B_No_Repair_but_NG", "B_Repair_OK"]
        
        self.table_and_fig_gap = -0.65
        
        self.color_dict = {
                'R_NG': 'fuchsia',
                'G_NG': 'mediumseagreen',
                'B_NG': 'blue',
                'R_Yield': 'fuchsia', 
                'G_Yield': 'mediumseagreen', 
                'B_Yield': 'blue',
                'Avg_Yield': 'black'
        }
        
        self.MB_OPID_ls = ['TNLBO', 'TNABO']


    def CreateLog(self, fileName, logPath):
        if not os.path.exists(logPath):
            os.mkdir(logPath)
            logging.basicConfig(
                filename= f'./log/{fileName}', 
                filemode= 'w', 
                format= '%(asctime)s - %(message)s', 
                encoding= 'utf-8'
                )
        else:
            logging.basicConfig(
                filename= f'./log/{fileName}', 
                filemode='a', 
                format='%(asctime)s - %(message)s',
                encoding='utf-8'
                )


    def readDataframeFromMongoDB(self, MODEL: str, Inspection_Type:str):
        """
        If need to change the report of model, please enter the product size.
        
        Parameters:
        ------------
        MODEL: Enter 13.6 or 17.3 or 16.1
        
        Inspection_Type: L255, L10, L0, edge_dark_point 
        
        Return:
        -------
        
        pd.DataFrame: the dataframe filter from MongoDB   
        """
         
        cursor = self.collection.find(
            {}, 
            {'_id':0, 'HeatMap':0, 'MES_ID':0, 'TOOL_ID':0, 'ABBR_NO':0, 'ACTUAL_RECIPE':0, 'DefectCode_2D':0}
        )
        
        df = pd.DataFrame.from_records(cursor)
        df = df.sort_values(by=['CreateTime'])
        df['Defect_Code'] = df['Defect_Code'].fillna('NaN')
        df = df[(df["Defect_Code"] != 'NaN') & (df["Inspection_Type"] == Inspection_Type)]
        
        if str(MODEL)=='13.6':
            df = df[(~df["SHEET_ID"].str.startswith("VKV")) & (~df["SHEET_ID"].str.startswith("VXV"))]
        elif str(MODEL)=='16.1':
            df = df[df["SHEET_ID"].str.startswith("VKV")]
        elif str(MODEL)=='17.3':
            df = df[df["SHEET_ID"].str.startswith("VXV")]
            
        self.CreateLog(fileName='plot_recorder.log', logPath=self.logPath)
        
        return df


    # 在 sort_values 時, Defect_Code 也必須加入, 否則 defect_code 之間會有間隔, 在 keep 時會取得錯誤的資料
    def by24hours(self, df: pd.DataFrame) -> pd.DataFrame:
        now = datetime.now().replace().strftime('%Y%m%d%H%M')
        lastDay = (datetime.now().replace() - timedelta(hours=24)).strftime('%Y%m%d%H%M')
        by24hours_df = df[(df.CreateTime >= lastDay) & (df.CreateTime <= now)]
        
        filterSheetdf = by24hours_df.filter(self.key_list)  
        filterdf = filterSheetdf[~filterSheetdf.Defect_Code.isna()].sort_values(['CreateTime', 'LED_TYPE', 'Defect_Code'], ascending=False).drop_duplicates(['CreateTime', 'SHEET_ID', 'LED_TYPE', 'OPID'], keep='first').reset_index(drop=True)
        
        filterdf['CreateTime'] = filterdf.CreateTime.apply(lambda x: str(x)[:-1])
        
        return now, lastDay, filterdf


    def byDays(self, df: pd.DataFrame) -> pd.DataFrame:
        # set period of time
        now_date = datetime.now().replace()
        lastweek =  now_date - timedelta(days=7)

        lastweek_str = lastweek.strftime('%Y%m%d%H%M')
        now_date_str = now_date.strftime('%Y%m%d%H%M')

        # filter dataframe by columns
        filterSheetdf = df.filter(self.key_list)
        bydays_df_origin = filterSheetdf[(filterSheetdf.CreateTime >= lastweek_str) & (filterSheetdf.CreateTime <= now_date_str)]
        
        bydays_df = bydays_df_origin[~bydays_df_origin.Defect_Code.isna()].sort_values(['CreateTime', 'LED_TYPE', 'Defect_Code'], ascending=False).drop_duplicates(['CreateTime', 'SHEET_ID', 'LED_TYPE', 'OPID'], keep='first').reset_index(drop=True)
        
        # group dataframe
        groupby_column = ['CreateTime', 'OPID', 'SHEET_ID', 'LED_TYPE' ,'Grade', 'Yield', 'NGCNT', 'Bond_Success_Rate', 'LightingCheck_2D', 'Luminance_2D', 'YiledAnalysis_2D']
        
        bydays_df_gr = bydays_df.groupby(groupby_column)[['Process_OK', 'Process_NG', 'NO_Process_OK', 'NO_Process_NG']].aggregate(sum).reset_index()  
        
        bydays_df_gr['CreateTime'] = bydays_df_gr.CreateTime.apply(lambda x: str(x)[:-1])
        
        return now_date_str, lastweek_str, bydays_df_gr


    def Pivot_dataframe(self, *args:str, filterSheetdf:pd.DataFrame, set_values:str, Is_main_bond=False,
                        **kwargs):
        """Change the data structure and return the dataframe.
        If Is_main_bond is True, please set the columns to and enter the column name needed.

        The column name will be assign to the args.

        Set value is select original column to become value of new dataframe 

        if need to plot report pls set kwargs.
        
        Parameters:
        -----------
            args (str): there are value of columns you won't to chage.
            
            filterSheetdf (pd.DataFrame): the dataframe you want to process.
            
            set_values (str): which column you want to calculate.
            
            Is_main_bond (bool, optional): _description_. Defaults to True.

        Returns:
        --------
            pd.DataFrame: pivot_df
        
        kwargs: 
        --------
            plot_report = True
        """
        
        needed_list = ['CreateTime', 'NGCNT', 'SHEET_ID', 'LED_TYPE', 'Yield', 'Bond_Success_Rate', 'Grade']
        process_columns = ['Process_OK', 'Process_NG', 'NO_Process_OK', 'NO_Process_NG']
        
        group_dataframe = filterSheetdf.groupby(needed_list)[process_columns].aggregate(sum).reset_index()
        
        group_dataframe['Total_CNT'] = group_dataframe["Process_OK"] + group_dataframe['Process_NG'] + group_dataframe['NO_Process_OK'] + group_dataframe['NO_Process_NG']
        
        group_dataframe = group_dataframe.reset_index(drop=True)
        
        pivot_df = pd.pivot_table(
            data=group_dataframe, index=[*args], columns=['LED_TYPE'], values=set_values, sort=False, fill_value=0
        )
        
        if 'R' not in pivot_df.columns:
            pivot_df["R"] = 0
        if 'G' not in pivot_df.columns:
            pivot_df["G"] = 0
        if 'B' not in pivot_df.columns:
            pivot_df["B"] = 0

        # reorder by LED type
        pivot_df = pivot_df[['R', 'G', 'B']]
           
        for i in pivot_df.columns:
            pivot_df[str(i)] = pivot_df[str(i)].astype('int')
        pivot_df = pivot_df.reset_index()
        
        # 因為 main bond 的 只有 process的類別 所以不需要另外算
        # 但因為非 main bond 的站點, 關係到 process 和 no process, 所以兩者的算法需要做分別
        # 非 main bond 的站點 需要做兩次 pivot 將 process ok 和 process ng 的數量先做總和再算良率
        if Is_main_bond == True:
            pivot_df['R_Yield'] = ((pivot_df['Total_CNT']-pivot_df['R']) / (pivot_df['Total_CNT']))*100
            pivot_df['G_Yield'] = ((pivot_df['Total_CNT']-pivot_df['G']) / (pivot_df['Total_CNT']))*100
            pivot_df['B_Yield'] = ((pivot_df['Total_CNT']-pivot_df['B']) / (pivot_df['Total_CNT']))*100
            pivot_df['Avg_Yield'] = ((pivot_df['Total_CNT']*3-pivot_df['R']-pivot_df['G']-pivot_df['B'])/(pivot_df['Total_CNT']*3))*100 
            pivot_df['Avg_Yield'] = pivot_df['Avg_Yield'].apply(lambda x: math.floor(x*100)/100.0)
            
        else:
            pivot_df['R_Yield'] = ((pivot_df['Total_CNT']-pivot_df['R']) / (pivot_df['Total_CNT']))*100
            pivot_df['G_Yield'] = ((pivot_df['Total_CNT']-pivot_df['G']) / (pivot_df['Total_CNT']))*100
            pivot_df['B_Yield'] = ((pivot_df['Total_CNT']-pivot_df['B']) / (pivot_df['Total_CNT']))*100
            
            pivot_list = ['SHEET_ID', 'CreateTime', 'Total_CNT']
            
            df_process_ok = pd.pivot_table(data=group_dataframe, index=pivot_list, columns=['LED_TYPE'], values='Process_OK', sort=False)
            
            df_process_ng = pd.pivot_table(data=group_dataframe, index=pivot_list, columns=['LED_TYPE'], values='Process_NG', sort=False)
            
            del group_dataframe
            
            merged_df = df_process_ok.merge(df_process_ng, on=pivot_list, how='left')
            merged_df = merged_df.fillna(0)
    
            # if merge df did not have column below
            assign_zero_col = ['R_x', 'G_x', 'B_x', 'R_y', 'G_y', 'B_y']
            for i in assign_zero_col:
                if i not in merged_df.columns:
                    merged_df[i] = 0
                else:
                    continue
            
            total_defect_cnt = merged_df['R_x'] + merged_df['G_x'] + merged_df['B_x']
            total_led_cnt = merged_df['R_x'] + merged_df['G_x'] + merged_df['B_x'] + merged_df['R_y'] + merged_df['G_y'] + merged_df['B_y']
            
            merged_df['Repair_Success_Rate'] = (total_defect_cnt / total_led_cnt)*100
            
            del total_defect_cnt, total_led_cnt
            
            pivot_df = merged_df.merge(pivot_df, on=pivot_list, how='left')
            pivot_df = pivot_df.fillna(100)
            pivot_df['Repair_Success_Rate'] = pivot_df['Repair_Success_Rate'].apply(lambda x: math.floor(x*100)/100.0)
            
            # 畫圖才執行以下程式碼
            if kwargs.get('plot_report', False) == True:
                pivot_df['Avg_Yield'] = (((pivot_df['Total_CNT']*3)-(pivot_df['R']+pivot_df['G']+pivot_df['B'])) / (pivot_df['Total_CNT']*3))*100
            
        pivot_df['R_Yield'] = pivot_df['R_Yield'].apply(lambda x: math.floor(x*100)/100.0)
        pivot_df['G_Yield'] = pivot_df['G_Yield'].apply(lambda x: math.floor(x*100)/100.0)
        pivot_df['B_Yield'] = pivot_df['B_Yield'].apply(lambda x: math.floor(x*100)/100.0)
        
        if Is_main_bond==False & kwargs.get('plot_report', False) == True:      
            pivot_df['Avg_Yield'] = pivot_df['Avg_Yield'].apply(lambda x: math.floor(x*100)/100.0)
            
        return pivot_df


    def ChooseReportType(self, summary_df, Model_type, choose_report_type:str):
        if choose_report_type.lower()=='daily':
            now_date, lastweek, res = self.by24hours(summary_df)
            figname_type = 'byPer24Hour'
            
        elif choose_report_type.lower()=='weekly':
            now_date, lastweek, res = self.byDays(summary_df)
            figname_type = 'byDay'
            lastweek = str(lastweek).split()[0]
            now_date = str(now_date).split()[0]
        
        if len(res.index) != 0:
            resOPIDls = res.OPID.tolist()
            resOPIDls = list(dict.fromkeys(resOPIDls))
            df_ls = []
            
            for OPID in resOPIDls:
                if OPID[0] != 'T':
                    logging.warning(f'[SKIP] OPID {OPID} was skipped')
                    continue
                
                elif OPID in self.MB_OPID_ls:
                    filterdf = res[res['OPID']==OPID].reset_index(drop=True)
                    filterdf = self.Pivot_dataframe(
                        'CreateTime', 'SHEET_ID', 'Grade', 'Total_CNT',
                        filterSheetdf=filterdf, 
                        set_values='NGCNT', 
                        Is_main_bond=True,
                        plot_report = True
                    )
                    filterdf = filterdf.drop(['Total_CNT'],  axis=1)
                    df_ls.append(filterdf)
                    
                else:
                    filterdf = res[res['OPID']==OPID].reset_index(drop=True)
                    
                    filterdf = self.Pivot_dataframe(
                        'CreateTime', 'SHEET_ID', 'Grade', 'Total_CNT', 
                        filterSheetdf=filterdf, 
                        set_values='NGCNT',  # indicated R, G, B
                        Is_main_bond=False,
                        plot_report=True
                    )
                    
                    filterdf = filterdf.drop(['Total_CNT','G_x','B_x','R_x','G_y','B_y','R_y'],  axis=1)
                    df_ls.append(filterdf)

                FullOPID = self.OPID_comparison_table.get(OPID, OPID)
                if len(filterdf.index)==0:
                    continue
                else:
                    every_date = [str(i)[4:-3] for i in filterdf.CreateTime.tolist()]
                    R_NG = filterdf.R.tolist()
                    G_NG = filterdf.G.tolist()
                    B_NG = filterdf.B.tolist()
                    R_Yield_ls = filterdf.R_Yield.tolist()
                    G_Yield_ls = filterdf.G_Yield.tolist()
                    B_Yield_ks = filterdf.B_Yield.tolist()
                    sheet_label = filterdf.SHEET_ID.tolist()
                    grade_label = filterdf.Grade.tolist()
                    table_and_fig_gap = self.table_and_fig_gap
                    
                    if OPID in self.MB_OPID_ls:
                        columnName1 = 'Avg_Yield'
                    else:
                        columnName1 = 'Repair_Success_Rate'
                        
                    self.plotTable(
                        R_NG, G_NG, B_NG, R_Yield_ls, G_Yield_ls, B_Yield_ks,
                        figName = f"{FullOPID}_{datetime.today().weekday()}_{figname_type}.jpg",
                        filterSheetdf = filterdf,
                        columnName1 = columnName1,
                        columnName2 = 'R',
                        columnName3 = 'G',
                        columnName4 = 'B',
                        columnName5 = 'Avg_Yield',
                        sheet_label = sheet_label,
                        date_label = every_date,
                        grade_label = grade_label,
                        figTitle = f'{Model_type}" {FullOPID} Yield Trend Chart {lastweek[4:-4]}~{now_date[4:-4]}',
                        xticksRotation = 0,
                        RGBlabel = None,
                        rowLabels = ['R_NG', 'G_NG', 'B_NG', 'R_Yield', 'G_Yield', 'B_Yield'],
                        table_bbox = [0, table_and_fig_gap, 1, 0.4],
                        bar_label_rotation = 0,
                        OPID=OPID,
                        Model_type=Model_type
                    )
                    
            if len(df_ls) != 0:            
                temp_df = pd.concat(df_ls)
            else:
                temp_df = pd.DataFrame()
                
            if Model_type == 13.6:
                pd.DataFrame.to_csv(temp_df, self.reportImgPath136 + 'TempDateFrame.csv')  
            elif Model_type == 16.1:
                pd.DataFrame.to_csv(temp_df, self.reportImgPath161 + 'TempDateFrame.csv')
            elif Model_type == 17.3:
                pd.DataFrame.to_csv(temp_df, self.reportImgPath173 + 'TempDateFrame.csv')  
                
        else:
            logging.warning(f'MODEL: {Model_type}, type: {choose_report_type}, No data from {lastweek} to {now_date}')
            
        return res


    def set_ylim_Using_standard_deviation(self, list1:list):
        
        ymin = 0
        arr = np.asarray(list1)
        
        if len(arr)==1:
            ymin = 0
        else:    
            arr_std = arr.std(axis=0)
            arr_std = float(math.floor((arr_std*100)/100.0))
            arr_max = arr.max()
            arr_min = arr.min()
            
            if arr_std <= 1:
                ymin = arr_min - 20*(arr_std)
            if arr_std >= 1:
                ymin = arr_min - 1.05*(arr_max-arr_min)
            if ymin <= 0:
                ymin = 0
                
        return int(ymin)


    def checkSheeIDLength(self, SheetID_list: list):
        length = 6
        
        for i in SheetID_list:
            if len(i) >= 8:
                length = len(i)
                break
            
        return length


    def plotTable(self, *args, figName: str, filterSheetdf: pd.DataFrame, columnName1: str, 
                  columnName2: str, columnName3: str, columnName4: str, sheet_label=None, date_label=None, grade_label=None, xlabel=None, **kwargs):
        """The agrs is corresponding to celltext, each columnName will be created to a dataframe and plot the line or bar chart.

        The columnNames will be use like: 

                filterSheetdf[[columnName1]].plot(kind='line', marker='o', color ='tan', ylim=(0,140), ax=ax1, legend=False)

                filterSheetdf[[columnName2]].plot(kind='line', marker='d', color ='salmon', ylim=(0,120), ax=ax2, legend=False)
        
                filterSheetdf[[columnName3, columnName4]].plot(kind='bar', stacked=True, ylim=(0, NGheight), ax=ax3, legend=False)

        sheet_label should be a empty list or text list

        kwargs:
        --------
            figTitle: str
            xlabelRotation: int
            RGBlabel: list | None
            bar_label_rotation: int

            ---Matplotlib table's params is as shown below---

            rowLabels: list
            colLabels: list | None
            table_bbox: list
                
        """
        
        colors = [self.R_color, self.G_color, self.B_color]
        Model_type = kwargs.get('Model_type')
        
        if kwargs.get('OPID', None) == 'TNLBO':
            ID_length = self.checkSheeIDLength(sheet_label)
            if ID_length < 8:
                if len(sheet_label) <= 16:
                    fig, ax1 = plt.subplots(figsize=(10, 5))
                elif len(sheet_label) > 16 & len(sheet_label) <= 30:
                    fig, ax1 = plt.subplots(figsize=(26, 5))
                else:
                    fig, ax1 = plt.subplots(figsize=(35, 5))
            else:
                if len(sheet_label) <= 7:
                    fig, ax1 = plt.subplots(figsize=(15, 5))
                elif len(sheet_label) > 7 & len(sheet_label) <= 16:
                    fig, ax1 = plt.subplots(figsize=(50, 5))
                elif len(sheet_label) > 16 & len(sheet_label) <= 30:
                    fig, ax1 = plt.subplots(figsize=(100, 5))
                elif len(sheet_label) > 30:
                    fig, ax1 = plt.subplots(figsize=(170, 5))
            del ID_length

            ymin = self.set_ylim_Using_standard_deviation(filterSheetdf[columnName1].values.tolist())

            ax1.set_zorder(2)
            ax1.set_facecolor('none')
            ax2 = ax1.twinx()
            ax2.set_zorder(1) 
            ax2.set_facecolor('none')
            ax3 = ax1.twinx()
            ax3.set_zorder(3) 
            ax3.set_facecolor('none')
            ax3.set_visible(False)
            
            filterSheetdf[[columnName1]].plot(kind='line', marker='o', color ='black', ylim=(ymin, 100), ax=ax1, legend=False)
            filterSheetdf[[columnName2, columnName3, columnName4]].plot(kind='bar', stacked=False, color=colors, ax=ax2, ylim=(0, 400), legend=False)
            
            filterSheetdf['target'] = 99.8
            filterSheetdf['target'].plot(kind='line', color ='red', ylim=(ymin, 100), ax=ax1, legend=False)
            
            # ax1.set_xticklabels(filterSheetdf.SHEET_ID.tolist(), rotation=0)

            figTitle = kwargs.get('figTitle', 'Sample')
            
            
            if xlabel != None:
                ax1.set_xlabel(xlabel, fontsize=14)
            ax1.set_ylabel('Yiled', fontsize=14)

            rot = kwargs.get('xticksRotation', 0)
            xticks = []
            if sheet_label != None and rot != None:
                for i in range(len(sheet_label)):
                    total = date_label[i] + '\n\n' + sheet_label[i] + '\n\n' + grade_label[i]
                    xticks.append(total)
                # print(xticks)
                ax1.set_xticklabels(xticks, rotation = rot)
                

            ax2.set_title(f"{figTitle}", fontsize=20, pad=60)
            ax2.set_ylabel("NG_COUNT", fontsize=14)
            for xtick, y in zip(filterSheetdf.index, filterSheetdf['Avg_Yield'].tolist()):
                if y == 0:
                    continue
                ax1.text(x=xtick, y=y+0.02, s=f'{y:.2f}%', ha="center", va='bottom', fontsize=8, rotation=0)
            
            the_table = ax1.table(
                cellText = args,
                rowLabels = kwargs.get('rowLabels'),
                colLabels = kwargs.get('colLabels', None),
                cellLoc='center',
                rowColours =['white']*len(kwargs.get('rowLabels')),
                bbox = kwargs.get('table_bbox'),
            )

            table_props = the_table.properties()
            table_cells = table_props['children']
            del table_props
            
            for cell in table_cells: 
                cell.get_text().set_color(self.color_dict.get(cell.get_text().get_text(), 'black'))

            labels = ['MB Yield', 'Target', 'R_NG', 'G_NG', 'B_NG']
            the_table.auto_set_font_size(False)
            the_table.set_fontsize(8)
            fig.legend(loc='upper center', labels=labels, bbox_to_anchor=(0.5, 1), ncol=len(labels), edgecolor='black')
            try:
                if Model_type == 13.6:
                    plt.savefig(f'{self.MainBondImgSavePath136 + figName}', bbox_inches='tight', dpi=100)
                if Model_type == 16.1:
                    plt.savefig(f'{self.MainBondImgSavePath161 + figName}', bbox_inches='tight', dpi=100)
                if Model_type == 17.3:
                    plt.savefig(f'{self.MainBondImgSavePath173 + figName}', bbox_inches='tight', dpi=100)
            except:
                pass
            plt.cla()
            plt.close(fig)
     

        else: 
            ID_length = self.checkSheeIDLength(sheet_label)
            if ID_length < 8:
                if len(sheet_label) <= 16:
                    fig, ax1 = plt.subplots(figsize=(10, 5))
                elif len(sheet_label) > 16 & len(sheet_label) <= 30:
                    fig, ax1 = plt.subplots(figsize=(25, 5))
                else:
                    fig, ax1 = plt.subplots(figsize=(35, 5))
            else:
                if len(sheet_label) <= 7:
                    fig, ax1 = plt.subplots(figsize=(15, 5))
                elif len(sheet_label) > 7 & len(sheet_label) <= 16:
                    fig, ax1 = plt.subplots(figsize=(50, 5))
                elif len(sheet_label) > 16 & len(sheet_label) <= 30:
                    fig, ax1 = plt.subplots(figsize=(100, 5))
                elif len(sheet_label) > 30:
                    fig, ax1 = plt.subplots(figsize=(170, 5))
            del ID_length
            
            ax1.set_zorder(2)
            ax1.set_facecolor('none')
            ax2 = ax1.twinx()
            ax2.set_zorder(1) 
            ax2.set_facecolor('none')
            ax3 = ax1.twinx()
            ax3.set_zorder(0)
            ax3.set_facecolor('none')


            filterSheetdf[[columnName1]].plot(kind='line', marker='o', color ='black', ylim=(0, 110), ax=ax1, legend=False)
            filterSheetdf[[kwargs.get('columnName5')]].plot(kind='line', marker='o', color ='saddlebrown', ylim=(0,120), ax=ax2, legend=False)
            filterSheetdf[[columnName2, columnName3, columnName4]].plot(kind='bar', stacked=False ,ylim=(0,400), color=colors, ax=ax3, legend=False)  
            filterSheetdf['target'] = 99.8

            figTitle = kwargs.get('figTitle', 'Sample')
            
            if xlabel != None:
                ax1.set_xlabel(xlabel, fontsize=14)
            ax1.set_ylabel('Light On Yield', fontsize=14)
            rot = kwargs.get('xticksRotation', 0)

            xticks = []
            if sheet_label != None and rot != None:
                for i in range(len(sheet_label)):
                    total = date_label[i] + '\n\n' + sheet_label[i] + '\n\n' + grade_label[i]
                    xticks.append(total)
                # print(xticks)
                ax1.set_xticklabels(xticks, rotation=rot)
      
            ax2.set_title(f"{figTitle}", fontsize=20, pad=60)
            ax2.spines['left'].set_position(('axes', -0.1))
            ax2.set_ylabel('DE/RE Yield (Success Ratio)', fontsize=14)
            ax2.yaxis.set_label_position("left")
            ax2.spines["left"].set_visible(True)
            ax2.yaxis.tick_left()

            ax3.set_ylabel("NG_COUNT", fontsize=14)

            for xtick, y in zip(filterSheetdf.index, filterSheetdf[columnName1].tolist()):
                if y == 0:
                    continue
                ax1.text(x=xtick, y=y+0.05, s=f'{y:.2f}%', ha="center", va='bottom', fontsize=8, rotation=0)

            for xtick, y in zip(filterSheetdf.index, filterSheetdf[kwargs.get('columnName5')].tolist()):
                if y == 0:
                    continue
                ax2.text(x=xtick, y=y+0.05, s=f'{y:.2f}%', ha="center", va='bottom', fontsize=8, rotation=0)

            the_table = ax1.table(
                cellText = args,
                rowLabels = kwargs.get('rowLabels'),
                colLabels = kwargs.get('colLabels', None),
                cellLoc='center',
                rowColours =['white']*len(kwargs.get('rowLabels')),
                bbox = kwargs.get('table_bbox'),
            )


            labels = ['Repair_Success_Rate', 'Avg_Yield', 'R_NG', 'G_NG', 'B_NG']
            the_table.auto_set_font_size(False)
            the_table.set_fontsize(8)

            table_props = the_table.properties()
            table_cells = table_props['children']
            for cell in table_cells: 
                cell.get_text().set_color(self.color_dict.get(cell.get_text().get_text(), 'black'))

            fig.legend(loc='upper center', labels=labels, bbox_to_anchor=(0.5, 1), ncol=len(labels), edgecolor='black')
            try:
                if Model_type == 13.6:
                    plt.savefig(f'{self.reportImgPath136 + figName}', bbox_inches='tight', dpi=100)
                if Model_type == 16.1:
                    plt.savefig(f'{self.reportImgPath161 + figName}', bbox_inches='tight', dpi=100)
                if Model_type == 17.3:
                    plt.savefig(f'{self.reportImgPath173 + figName}', bbox_inches='tight', dpi=100)
            except:
                pass
            plt.cla()
            plt.close(fig)
   
        
    def clear_old_data(self, MODEL):
        key_MB_IMG_dict = {
            13.6: './report_production/MainBondYiled13.6/',
            16.1: './report_production/MainBondYiled16.1/',
            17.3: './report_production/MainBondYiled17.3/'
        }
        key_model_path_dict = {
            13.6: './report_production/13.6/',
            16.1: './report_production/16.1/',
            17.3: './report_production/17.3/'
        }
        if not os.path.exists(key_MB_IMG_dict.get(MODEL)):
            os.makedirs(key_MB_IMG_dict.get(MODEL), exist_ok=True)
        elif not os.path.exists(key_model_path_dict.get(MODEL)):
            os.makedirs(key_model_path_dict.get(MODEL), exist_ok=True)
        else:
            shutil.rmtree(key_MB_IMG_dict.get(MODEL))
            shutil.rmtree(key_model_path_dict.get(MODEL))
            time.sleep(5)
            os.makedirs(key_MB_IMG_dict.get(MODEL), exist_ok=True)
            os.makedirs(key_model_path_dict.get(MODEL), exist_ok=True)


class ScatterStacked(TableType):
    """Stack defect scatter, the param is float percentage, and that used to remove the false defect. 

    Parameters:
    ------------
        vertical_param (float): The threshold value to remove false defect in vertical.
        Horizontal_param (float): The threshold value to remove false defect in horizontal.
    """
    def __init__(self, vertical_param:float, Horizontal_param:float):
        super(ScatterStacked, self).__init__()
        self.vertical_param = vertical_param
        self.Horizontal_param = Horizontal_param
        


    def removeFalseDefect(self, df:pd.DataFrame, OPID:str):
        """Remove vertical & Horizontal false defect line.
        And keep the point of defect coordinate belong to process ng.

        Parameters:
        -----------
            df (pd.DataFrame): Yield dataframe
            OPID (str): Operation ID

        Returns:
        --------
            pd.DataFrame: Removed false defect dataframe
        """
        
        df = pd.DataFrame(df)
        df.columns.astype('int')
        
        if OPID != "TNLBO":
            # remove vertical line
            for i in range(len(df.columns.tolist())):
                if (np.count_nonzero(df[i]==0) / len(df.index)) > self.vertical_param:
                    df[i] = np.where((df[i]==0) | (df[i]==1), 1, df[i]) 
            # remove Horizontal line
            df = np.asarray(df, dtype='uint8') 
            for j in range(df.shape[0]):
                if (np.count_nonzero(df[j]==0) / df.shape[1]) > self.Horizontal_param:
                    df[j] = np.where((df[j]==0) | (df[j]==1), 1, df[j])
            return df
        
        else:
            for i in range(len(df.columns.tolist())):
                if (np.count_nonzero(df[i]==10) / len(df.index)) > self.vertical_param:
                    df[i] = np.where((df[i]==10), 11, 1) 
            df = np.asarray(df, dtype='uint8') 
            for j in range(df.shape[0]):
                if (np.count_nonzero(df[j]==10) / df.shape[1]) > self.Horizontal_param:
                    df[j] = np.where((df[j]==10), 11, 1)
            return df

    
    def changeMaximumValue(self, df, OPID):
        """
        If the dataframe dose not have corresponding bonding data, the max value will be 1.

        Because of the reason, the dataframe need to be change the max value to 11.
        """
        arr = np.asarray(df, dtype=int)
        max_value = np.amax(arr)
        if max_value==1 and OPID in self.MB_OPID_ls: # 是 main bonding 全部都算 process
            new_df = np.where(df==1, 11, 10)
            new_df = pd.DataFrame(new_df)
            return new_df
        return df


    def get_yield_arr(self, OPID_df:pd.DataFrame, OPID:str):
        """Get 2D array from column -> YiledAnalysis_2D
        
        Parameter:
        -----------
            OPID_df (pd.DataFrame): specific color dataframe

        Returns:
            np.array: yield 2D arr
        """
        if len(OPID_df.index) != 0:
            Yield_arr = self.fs.get(ObjectId(OPID_df.iloc[0].YiledAnalysis_2D)).read()
            Yield_arr = pickle.loads(Yield_arr)
            Yield_arr = self.changeMaximumValue(Yield_arr, OPID)
        else:
            Yield_arr = pd.DataFrame()
            
        return Yield_arr
    
    
    def ProduceScatterPlot(self, df:pd.DataFrame, chooseTpye:str, MODEL:int):
        """
        Find the specific sheet ID data and pivot it.
        """
        
        if chooseTpye.lower() == 'daily':
            _, _, filterSheetdf = self.by24hours(df)
        if chooseTpye.lower() == 'weekly':
            _, _, filterSheetdf = self.byDays(df)
        
        NeedToProcessSheetID = []
        if len(filterSheetdf.index) != 0:
            OPID_ls = list(dict.fromkeys(filterSheetdf.OPID.tolist()))
            sheetID_ls = list(dict.fromkeys(filterSheetdf.SHEET_ID.tolist()))
            
            for OPID in OPID_ls:
                if OPID[0] != 'T':
                    continue
                else:
                    OPID_df = filterSheetdf[(filterSheetdf['OPID']==OPID)].reset_index(drop=True)

                for sheet in sheetID_ls:
                    filterdf = OPID_df[(OPID_df['SHEET_ID']==sheet)].reset_index(drop=True)
                    # 從時間最後面的排第一個
                    filterdf = filterdf.sort_values(['CreateTime', 'LED_TYPE'], ascending=False)
                    filterdf = filterdf.drop_duplicates(['SHEET_ID', 'LED_TYPE'], keep='first').reset_index(drop=True)
                    if len(filterdf.index) == 0:
                        continue
                    else:
                        if OPID == 'TNLBO':
                            NeedToProcessSheetID.append(sheet)
                            pivot_df = self.Pivot_dataframe(
                                'CreateTime', 'SHEET_ID', 'Total_CNT', 
                                filterSheetdf=filterdf, 
                                set_values='Process_NG', 
                                Is_main_bond=True
                            )
                            
                        else:
                            NeedToProcessSheetID.append(sheet)
                            pivot_df = self.Pivot_dataframe(
                                'CreateTime', 'SHEET_ID', 'Total_CNT',
                                filterSheetdf=filterdf, 
                                set_values='NGCNT', 
                                Is_main_bond=False
                            )

                        FullOPID = self.OPID_comparison_table.get(OPID, OPID)
                        figname = f'{sheet}_{FullOPID}_Defect_MAP'
                        print(filterdf)
                        R_OPID_df = filterdf[filterdf['LED_TYPE']=='R'].reset_index(drop=True)
                        G_OPID_df = filterdf[filterdf['LED_TYPE']=='G'].reset_index(drop=True)
                        B_OPID_df = filterdf[filterdf['LED_TYPE']=='B'].reset_index(drop=True)
                        R_Yield_arr = self.get_yield_arr(R_OPID_df, OPID)
                        G_Yield_arr = self.get_yield_arr(G_OPID_df, OPID)
                        B_Yield_arr = self.get_yield_arr(B_OPID_df, OPID)
                        
                        date = [str(i)[4:-3] for i in pivot_df.CreateTime.tolist()]
                        R_NG = pivot_df.R.tolist()
                        G_NG = pivot_df.G.tolist()
                        B_NG = pivot_df.B.tolist()
                        R_Yield_ls = pivot_df.R_Yield.tolist()
                        G_Yield_ls = pivot_df.G_Yield.tolist()
                        B_Yield_ls = pivot_df.B_Yield.tolist()

                        rowLabels = ['Date','R_NG', 'G_NG', 'B_NG', 'R_Yield', 'G_Yield', 'B_Yield']
                        # remove false defect
                        # self.StackScatterPlot(
                        #     sheet, 
                        #     FullOPID, 
                        #     R_Yield_arr, 
                        #     G_Yield_arr, 
                        #     B_Yield_arr, 
                        #     date, R_NG, G_NG, B_NG, R_Yield_ls, G_Yield_ls, B_Yield_ls,
                        #     removeFalseDefect=True, 
                        #     figname=figname, 
                        #     OPID=OPID,
                        #     rowLabels = rowLabels,
                        #     table_bbox = [1.1, 0, 0.15, 0.4],
                        #     MODEL=MODEL
                        # )

                        self.StackScatterPlot(
                            sheet, 
                            FullOPID, 
                            R_Yield_arr, 
                            G_Yield_arr, 
                            B_Yield_arr, 
                            date, R_NG, G_NG, B_NG, R_Yield_ls, G_Yield_ls, B_Yield_ls,
                            removeFalseDefect=False, 
                            figname=figname, 
                            OPID=OPID,
                            rowLabels = rowLabels,
                            table_bbox = [1.1, 0, 0.15, 0.4],
                            MODEL=MODEL
                        )
        return NeedToProcessSheetID                
    
    
    def check_yield_size(self, yield_df:pd.DataFrame, ymax:int, xmax:int) -> pd.DataFrame:
        """Check the yield dataframe and return it.

        Parameters:
        ------------
            yield_df (pd.DataFrame): Dataframe from 2D array
            ymax (str): product width
            xmax (str): product height

        Returns:
        --------
            pd.DataFrame: yield dataframe
        """
        
        if isinstance(yield_df, type(None)) or len(yield_df) == 0:
            yield_df = np.zeros((ymax, xmax), dtype=int)
        yield_df = pd.DataFrame(yield_df)
        return yield_df
       
                        
    def check_zero_matrix(self, arr:npt.ArrayLike):
        """Check the yield 2-D array whether is zero array
        
        If it is, then chage the value to one matrix.
        
        Because if matrix is zero matrix that mean there are no bonding data.

        Parameter:
        -----------
            arr (npt.ArrayLike): R or G or B 's Yield 2D array

        Returns:
            npt.ArrayLike: ones matrix or original matrix
        """
        if np.all(arr==0):
            arr = np.where(arr==0, 1, arr)
            return arr
        return arr
    
    
    def check_skip_pithes(self, arr:npt.ArrayLike, reduce_w:int, reduce_h:int):
        """Change the array to dataframe to check that whether is skip pitches product.

        Parameters:
        -----------
            arr (npt.ArrayLike): R or G or B Yield 2-D array
            reduce_w (int): the width after skip pitches
            reduce_h (int): the height after skip pitches
        """
        
        if np.all(arr==1):
            return arr
        
        orgin_arr_df = pd.DataFrame(arr)
        temp_arr_df = orgin_arr_df.copy()
        skip_pitch_w = reduce_w
        skip_pitch_h = reduce_h
        cnt_w, cnt_h = 0, 0
        
        # 調整轉置後取 2, 4, 6 row (原先是1, 3, 5)
        for index in range(len(orgin_arr_df.columns)):
            if index % 2 != 0:
                series_arr = np.asarray(orgin_arr_df[index], dtype=int)
                if np.all(series_arr==10) or np.all(series_arr==0):
                    cnt_w += 1
                
        for index, row in orgin_arr_df.iterrows():
            if index % 2 != 0:
                row_arr = np.asarray(row, dtype=int)
                if np.all(row_arr==10) or np.all(row_arr==0):
                    cnt_h += 1
                
        # 如果有跳 pitches 就將有跳過的地方改為1
        # 以原本的dataframe為基準, 將其改變後用 temp dataframe 取代
        if cnt_w == skip_pitch_w and cnt_h == skip_pitch_h:
            
            # 調整轉置後 column 的 serise
            for col in range(len(orgin_arr_df.columns)):
                if col % 2 != 0:
                    series_arr = np.asarray(temp_arr_df[col], dtype=int)
                    if np.all(series_arr==10):
                        temp_arr_df[col] = np.where(temp_arr_df[col]==10, 1, temp_arr_df[col])
                    elif np.all(series_arr==0):
                        temp_arr_df[col] = np.where(temp_arr_df[col]==0, 1, temp_arr_df[col])
                        
            # 調整轉置後 rows 的 seriese 取 2,4,6 row
            for index, row in orgin_arr_df.iterrows():
                if index % 2 != 0:
                    row_arr = np.asarray(row, dtype=int)
                    if np.all(row_arr==10):
                        temp_arr_df.loc[index] = np.where(row_arr==10, 1, row_arr)
                    elif np.all(row_arr==0):
                        temp_arr_df.loc[index] = np.where(row_arr==0, 1, row_arr)
                        
            return np.asarray(temp_arr_df, dtype=int)
        return arr
    
    
    def get_defect_coord(self, object_id:str, reduce_w:int, reduce_h:int):
        """Get Defect coordinates from lighting check 2D array from ObjectID
        
        Args:
        -------
            object_id (str): Object_ID
            reduce_w (int): skip pitches sheet was decrease the width of resolution 
            reduce_h (int): skip pitches sheet was decrease the height of resolution

        Returns:
        ---------
            list: x and y defect list
        """
        
        arr = self.fs.get(ObjectId(object_id)).read()
        arr = pickle.loads(arr)
        arr = self.check_skip_pithes(arr, reduce_w=reduce_w, reduce_h=reduce_h)
        arr = np.where(arr==0)
        x_coord = arr[1]
        y_coord = arr[0]
        
        return list(x_coord), list(y_coord)
    
    
    def get_luminance_2D(self, object_id:str, LED_TYPE:str):
        """Get luminance 2D array from ObjectID

        Args:
        ------
            object_id (str): Object_ID
            LED_TYPE (str): R / G / B

        Returns:
        ---------
            array: lum 2D array
            cmap: color list map
            int: max value of luminance 2D
            int: min value of luminance 2D
        """
        import matplotlib.colors as mcolors
        
        lum_arr = self.fs.get(ObjectId(object_id)).read()
        lum_arr = pickle.loads(lum_arr)
        
        if LED_TYPE =='R':
            colors = ['#000000', '#ffffff', '#800000', '#FF0000']
            
        elif LED_TYPE =='G':
            colors = ['#000000', '#ffffff', '#adff2f', '#006400']
            
        elif LED_TYPE =='B':
            colors = ['#000000', '#ffffff', '#add8e6', '#0000FF']

        cmap = mcolors.LinearSegmentedColormap.from_list('CMAP', colors)
        lum_max = np.amax(lum_arr)
        lum_min = np.amin(lum_arr)
        
        return lum_arr, cmap, lum_max, lum_min
    
    
    def get_NGCNT_Yield_list(df:pd.DataFrame, LED_TYPE:str):
        """Combine the all NGCNT and Yield result from inspection type.
        
        The list will be order from ins_ls then put in cell text.

        Args:
        ------
            df (pd.DataFrame): specifict sheet ID dataframe
            LED_TYPE (str): specifict LED_TYPE

        >>> ins_ls = ['L255', 'edge_Dark_point', 'L0', 'L10']
        >>> B_pattern_ls = get_NGCNT_Yield(df=df, LED_TYPE='B')
        >>> B_pattern_ls
            [[928, 99.28], [18, 98.8], [53, 99.95], [5639, 95.64]]
        
        Returns:
        ---------
            list: _description_
        """
        
        ins_ls = ['L255', 'edge_Dark_point', 'L0', 'L10']
        order_ls = []
        
        for ins in ins_ls:
            normal = df[(df['LED_TYPE']==LED_TYPE) & (df['Inspection_Type']==ins)]
            normal_ngcnt = normal.NGCNT.tolist()
            normal_yield = normal.Yield.tolist()
            fianl = normal_ngcnt + normal_yield
            order_ls.append(fianl)
            
        return order_ls
    
    
    def StackScatterPlot(self, sheetID:str, FullOPID:str, R_Yield:pd.DataFrame, G_Yield:pd.DataFrame,
                         B_Yield:pd.DataFrame, *args:list, removeFalseDefect:bool, **kwargs):
        """Stacked scatter is plotted following sheet ID that is the x axis of the daily and weekly report.
    
        Parameters:
        ------------
            sheetID (str): the sheet of product
            FullOPID (str): the full operatation ID at OPID comparison table
            R_Yield (npt.ArrayLike): 2D Yiled array of R
            G_Yield (npt.ArrayLike): 2D Yiled array of G
            B_Yield (npt.ArrayLike): 2D Yiled array of G
            removeFalseDefect (bool): If True, the flase defect will be remove.
            *args (list): the count of lists should be equal to row labels 
            
        kwargs:
        -------
            figname: the name of save scatter plot.
            OPID: the OPID will be used to regonize the scatter plot in which operation.
        """
        
        xymin = 0
        MODEL = kwargs.get("MODEL")
        
        if MODEL == 13.6:
            xmax = 480
            ymax = 270
            
        if MODEL == 16.1:
            xmax = 540
            ymax = 240
            
        if MODEL == 17.3:
            xmax = 1280
            ymax = 720
        
        # 不管是否為 main bonding 其 dataframe 都需要 return
        R_Yield = self.check_yield_size(R_Yield, ymax, xmax)
        G_Yield = self.check_yield_size(G_Yield, ymax, xmax)
        B_Yield = self.check_yield_size(B_Yield, ymax, xmax)
        
        R_Yield = np.asarray(R_Yield, dtype='int')
        G_Yield = np.asarray(G_Yield, dtype='int')
        B_Yield = np.asarray(B_Yield, dtype='int')
    
        # 確認是否為0矩陣 (因為缺少repair的資料 所以用一開始用0矩陣代替)
        # 如果是, return 1 矩陣
        R_Yield = self.check_zero_matrix(R_Yield)
        G_Yield = self.check_zero_matrix(G_Yield)
        B_Yield = self.check_zero_matrix(B_Yield)
        
        # 確認是否有pitches
        reduce_w = 240
        reduce_h = 135
        
        R_Yield = self.check_skip_pithes(R_Yield, reduce_w, reduce_h)
        G_Yield = self.check_skip_pithes(G_Yield, reduce_w, reduce_h)
        B_Yield = self.check_skip_pithes(B_Yield, reduce_w, reduce_h)
        del reduce_w, reduce_h

        OPID = kwargs.get('OPID')
        
        if OPID in self.MB_OPID_ls:
            if removeFalseDefect == True:
                R_Yield = self.removeFalseDefect(R_Yield, OPID)
                G_Yield = self.removeFalseDefect(G_Yield, OPID)
                B_Yield = self.removeFalseDefect(B_Yield, OPID)
            RPNG = np.where(R_Yield == 10) # coordinate
            GPNG = np.where(G_Yield == 10) # coordinate
            BPNG = np.where(B_Yield == 10) # coordinate
            
        else:
            if removeFalseDefect == True:
                R_Yield = self.removeFalseDefect(R_Yield, OPID)
                G_Yield = self.removeFalseDefect(G_Yield, OPID)
                B_Yield = self.removeFalseDefect(B_Yield, OPID) 

            RPNG = np.where(R_Yield == 10) # coordinate
            RPOK = np.where(R_Yield == 11)
            RNPNG = np.where(R_Yield == 0)
            
            GPNG = np.where(G_Yield == 10) # coordinate
            GPOK = np.where(G_Yield == 11)
            GNPNG = np.where(G_Yield == 0)
            
            BPNG = np.where(B_Yield == 10) # coordinate
            BPOK = np.where(B_Yield == 11)
            BNPNG = np.where(B_Yield == 0)   

        fig, ax = plt.subplots(figsize=(10, 5))
        
        ax.grid()
        ax.set_zorder(2)
        ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9 = ax.twinx(), ax.twinx(), ax.twinx(), ax.twinx(), ax.twinx(), ax.twinx(), ax.twinx(), ax.twinx()
        ax2.set_zorder(0) 
        ax3.set_zorder(2)
        ax4.set_zorder(0)
        ax5.set_zorder(2)
        ax6.set_zorder(0)
        ax7.set_zorder(2)
        ax8.set_zorder(2)
        ax9.set_zorder(2)

        ax.set_title(f"{sheetID}_{FullOPID} RGB Defect Map")
        ax.set_facecolor('none')
        ax2.set_facecolor('none')
        ax3.set_facecolor('none')
        ax4.set_facecolor('none')
        ax5.set_facecolor('none')
        ax6.set_facecolor('none')
        ax7.set_facecolor('none')
        ax8.set_facecolor('none')
        ax9.set_facecolor('none')

        ax2.set_yticklabels([])
        ax3.set_yticklabels([])
        ax4.set_yticklabels([])
        ax5.set_yticklabels([])
        ax6.set_yticklabels([])
        ax7.set_yticklabels([])
        ax8.set_yticklabels([])
        ax9.set_yticklabels([])

        ax.set_xlim([xymin, xmax])
        ax.set_ylim([xymin, ymax])
        ax2.set_ylim([xymin, ymax])
        ax3.set_ylim([xymin, ymax])
        ax4.set_ylim([xymin, ymax])
        ax5.set_ylim([xymin, ymax])
        ax6.set_ylim([xymin, ymax])
        ax7.set_ylim([xymin, ymax])
        ax8.set_ylim([xymin, ymax])
        ax9.set_ylim([xymin, ymax])

        if OPID in self.MB_OPID_ls:
            labels = ['R_Process_NG', 'G_Process_NG', 'B_Process_NG']
            ax.scatter(list(RPNG[1]), list(RPNG[0]), s=50, marker='.', edgecolors='lightcoral', facecolors='none')
            ax2.scatter(list(GPNG[1]), list(GPNG[0]), s=50, marker='.', edgecolors='forestgreen', facecolors='none')
            ax3.scatter(list(BPNG[1]), list(BPNG[0]), s=50, marker='.', edgecolors='dodgerblue', facecolors='none')
            ax.invert_yaxis()
            ax2.invert_yaxis()
            ax3.invert_yaxis()
            
            the_table = ax.table(
                cellText = args,
                rowLabels = kwargs.get('rowLabels'),
                colLabels = kwargs.get('colLabels', None),
                cellLoc='center',
                rowColours =['white']*len(kwargs.get('rowLabels')),
                bbox = kwargs.get('table_bbox'),
            )
            
            the_table.auto_set_font_size(False)
            the_table.set_fontsize(8)
            
            table_props = the_table.properties()
            table_cells = table_props['children']
            
            for cell in table_cells: 
                cell.get_text().set_color(self.color_dict.get(cell.get_text().get_text(), 'black'))
            fig.legend(loc='upper left', labels=labels, bbox_to_anchor=(0.93, -0.1, 1, 1), edgecolor='black')
            
        else:
            ax.scatter(list(RPNG[1]), list(RPNG[0]), s=50, marker=',', edgecolors='lightcoral', facecolors='none')
            ax2.scatter(list(RNPNG[1]), list(RNPNG[0]), s=50, marker='.', edgecolors='red', facecolors='none')
            ax3.scatter(list(RPOK[1]), list(RPOK[0]), s=50, marker='o', edgecolors='brown', facecolors='none')
            ax4.scatter(list(GPNG[1]), list(GPNG[0]), s=50, marker=',', edgecolors='forestgreen', facecolors='none')
            ax5.scatter(list(GNPNG[1]), list(GNPNG[0]), s=50, marker='.', edgecolors='green', facecolors='none')
            ax6.scatter(list(GPOK[1]), list(GPOK[0]), s=50, marker='o', edgecolors='lime', facecolors='none')
            ax7.scatter(list(BPNG[1]), list(BPNG[0]), s=50, marker=',', edgecolors='dodgerblue', facecolors='none')
            ax8.scatter(list(BNPNG[1]), list(BNPNG[0]), s=50, marker='.', edgecolors='blue', facecolors='none')
            ax9.scatter(list(BPOK[1]), list(BPOK[0]), s=50, marker='o', edgecolors='royalblue', facecolors='none')

            ax.invert_yaxis()
            ax2.invert_yaxis()
            ax3.invert_yaxis()
            ax4.invert_yaxis()
            ax5.invert_yaxis()
            ax6.invert_yaxis()
            ax7.invert_yaxis()
            ax8.invert_yaxis()
            ax9.invert_yaxis()
            
            the_table = ax.table(
                cellText = args,
                rowLabels = kwargs.get('rowLabels'),
                colLabels = kwargs.get('colLabels', None),
                cellLoc='center',
                rowColours =['white']*len(kwargs.get('rowLabels')),
                bbox = kwargs.get('table_bbox'),
            )
            
            the_table.auto_set_font_size(False)
            the_table.set_fontsize(8)
            
            table_props = the_table.properties()
            table_cells = table_props['children']
            
            for cell in table_cells: 
                cell.get_text().set_color(self.color_dict.get(cell.get_text().get_text(), 'black'))
            fig.legend(loc='upper left', labels=self.labels, bbox_to_anchor=(0.93, -0.1, 1, 1), edgecolor='black')
            
        figname = kwargs.get('figname')

        if removeFalseDefect==True:
            if MODEL==13.6:
                plt.savefig(f'{self.reportImgPath136 + figname}_rmDefect.png', bbox_inches='tight', dpi=100)
            if MODEL==16.1:
                plt.savefig(f'{self.reportImgPath161 + figname}_rmDefect.png', bbox_inches='tight', dpi=100)
            if MODEL==17.3:
                plt.savefig(f'{self.reportImgPath173 + figname}_rmDefect.png', bbox_inches='tight', dpi=100)
        else:
            if MODEL==13.6:
                plt.savefig(f'{self.reportImgPath136 + figname}_original.png', bbox_inches='tight', dpi=100)
            if MODEL==16.1:
                plt.savefig(f'{self.reportImgPath161 + figname}_original.png', bbox_inches='tight', dpi=100)
            if MODEL==17.3:
                plt.savefig(f'{self.reportImgPath173 + figname}_original.png', bbox_inches='tight', dpi=100)
        
        plt.cla()
        plt.close(fig)



class plot_single_RGB_scatter(ScatterStacked):
    def __init__(self):
        super(plot_single_RGB_scatter, self).__init__()
        
        
    def get_defect_coord(self, object_id:str, reduce_w:int, reduce_h:int):
        """Get Defect coordinates from lighting check 2D array from ObjectID
        
        Args:
        -------
            object_id (str): Object_ID
            reduce_w (int): skip pitches sheet was decrease the width of resolution 
            reduce_h (int): skip pitches sheet was decrease the height of resolution

        Returns:
        ---------
            list: x and y defect list
        """
        arr = self.fs.get(ObjectId(object_id)).read()
        arr = pickle.loads(arr)
        arr = self.check_skip_pithes(arr, reduce_w=reduce_w, reduce_h=reduce_h)
        arr = np.where(arr==0)
        x_coord = arr[1]
        y_coord = arr[0]
        return list(x_coord), list(y_coord)
    
    
    def get_luminance_2D(self, object_id:str, LED_TYPE:str):
        """Get luminance 2D array from ObjectID

        Args:
        ------
            object_id (str): Object_ID
            LED_TYPE (str): R / G / B

        Returns:
        ---------
            array: lum 2D array
            cmap: color list map
            int: max value of luminance 2D
            int: min value of luminance 2D
        """
        import matplotlib.colors as mcolors
        lum_arr = self.fs.get(ObjectId(object_id)).read()
        lum_arr = pickle.loads(lum_arr)
        
        if LED_TYPE =='R':
            colors = ['#000000', '#FF0000']
        elif LED_TYPE =='G':
            colors = ['#000000', '#00FF00']
        elif LED_TYPE =='B':
            colors = ['#000000', '#0000FF']

        cmap = mcolors.LinearSegmentedColormap.from_list('CMAP', colors)
        lum_max = np.amax(lum_arr)
        lum_min = np.amin(lum_arr)
        return lum_arr, cmap, lum_max, lum_min
    
    
    def get_NGCNT_Yield_list(df:pd.DataFrame, LED_TYPE:str):
        """Combine the all NGCNT and Yield result from inspection type.
        
        The list will be order from ins_ls then put in cell text.

        Args:
        ------
            df (pd.DataFrame): specifict sheet ID dataframe
            LED_TYPE (str): specifict LED_TYPE

        >>> ins_ls = ['L255', 'edge_Dark_point', 'L0', 'L10']
        >>> B_pattern_ls = get_NGCNT_Yield(df=df, LED_TYPE='B')
        >>> B_pattern_ls
            [[928, 99.28], [18, 98.8], [53, 99.95], [5639, 95.64]]
        
        Returns:
        ---------
            list: _description_
        """
        ins_ls = ['L255', 'edge_Dark_point', 'L0', 'L10']
        order_ls = []
        for ins in ins_ls:
            normal = df[(df['LED_TYPE']==LED_TYPE) & (df['Inspection_Type']==ins)]
            normal_ngcnt = normal.NGCNT.tolist()
            normal_yield = normal.Yield.tolist()
            fianl = normal_ngcnt + normal_yield
            order_ls.append(fianl)
        return order_ls
    

def SendLostImageMessage(messagelist):
    # send alarm mail           
    text_massage = []
    if len(messagelist) != 0:
        for i in range(len(messagelist)):
            text = messagelist[i] + '<br><br>'
            text_massage.append(text)
        message = 'There is no image of Sheet ID below in the Ledimg Folder. !!!<br><br>' + ''.join(text_massage) 
        customMessageAutoMail().send(message)  


def plot24Hours(Model_choose:float):
    """The save folder will be cleaned and create a new file every day.

    Parameters:
    ------------
        Model_choose (float): the model type follow the product specifications
    """
    choose = TableType()
    scatterPlot = ScatterStacked(vertical_param=0.1, Horizontal_param=0.1)
    choose.clear_old_data(Model_choose)
    Summary_df = choose.readDataframeFromMongoDB(MODEL=Model_choose, Inspection_Type='L0') 
    filterDF = choose.ChooseReportType(Summary_df, Model_type=Model_choose, choose_report_type='daily')
    sheetID_ls = scatterPlot.ProduceScatterPlot(Summary_df, chooseTpye='daily', MODEL=Model_choose)
    sheetID_ls = set(sheetID_ls)
    lost_img = getLightOnResult(sheetID_ls, Model_choose)
    # PPTmain().dailyReport(MODEL=Model_choose)

    if Model_choose == 13.6:
        auto_mail().sendReport(choose.reportImgPath136 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Daily', MODEL=Model_choose)
    if Model_choose == 16.1:
        auto_mail().sendReport(choose.reportImgPath161 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Daily', MODEL=Model_choose)
    if Model_choose == 17.3:
        auto_mail().sendReport(choose.reportImgPath173 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Daily', MODEL=Model_choose)
    # SendLostImageMessage(lost_img)


def plotDay(Model_choose):
    """The save folder will be cleaned and create a new file every week.

    Parameters:
    ------------
        Model_choose (float): the model type follow the product specifications
    """
    choose = TableType()
    scatterPlot = ScatterStacked(vertical_param=0.1, Horizontal_param=0.1)
    lost_img = []

    choose.clear_old_data(Model_choose)
    Summary_df = choose.readDataframeFromMongoDB(MODEL=Model_choose, Inspection_Type='L255') 
    filterDF = choose.ChooseReportType(Summary_df, Model_type=Model_choose, choose_report_type='weekly')

    sheetID_ls = scatterPlot.ProduceScatterPlot(Summary_df, chooseTpye='weekly', MODEL=Model_choose)
 
    # sheetID_ls = set(sheetID_ls)
    # lost_img = getLightOnResult(sheetID_ls, Model_choose)
    # PPTmain().weeklyReport(MODEL=Model_choose)

    # if Model_choose == 13.6:
    #     auto_mail().sendReport(choose.reportImgPath136 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Weekly', MODEL=Model_choose)
    # if Model_choose == 16.1:
    #     auto_mail().sendReport(choose.reportImgPath161 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Weekly', MODEL=Model_choose)
    # if Model_choose == 17.3:
    #     auto_mail().sendReport(choose.reportImgPath173 + 'TempDateFrame.csv', messageForTableType=f'{Model_choose} Weekly', MODEL=Model_choose)
    # SendLostImageMessage(lost_img) 
        


if __name__ == '__main__':
    
    # plot24Hours(Model_choose=16.1)
    # plot24Hours(Model_choose=13.6)
    # plotDay(Model_choose=16.1)
    plotDay(Model_choose=13.6)

    # scheduler1 = schedule.Scheduler()
    # scheduler1.every().days.at("07:30").do(plot24Hours, Model_choose=13.6)
    # scheduler1.every().days.at("07:31").do(plot24Hours, Model_choose=16.1)
    # scheduler1.every().days.at("07:32").do(plot24Hours, Model_choose=17.3)
    
    # scheduler2 = schedule.Scheduler()
    # scheduler2.every().wednesday.at("07:45").do(plotDay, Model_choose=13.6)
    # scheduler2.every().wednesday.at("07:46").do(plotDay, Model_choose=16.1)
    # scheduler2.every().wednesday.at("07:47").do(plotDay, Model_choose=17.3)

    # while True:
    #     scheduler1.run_pending()
    #     scheduler2.run_pending()
    #     time.sleep(10)
        
        

