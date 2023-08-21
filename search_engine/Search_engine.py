import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from process_AOI_defect import Merge_LUM_and_AOI_Defect
import json

st.set_page_config(
    page_title="MT",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init() -> None:
    if 'lum_op_id_to_Options' not in st.session_state:
        st.session_state['lum_op_id_to_Options'] = []

    if 'lum_create_time_to_Options' not in st.session_state:
        st.session_state['lum_create_time_to_Options'] = []

    if 'aoi_op_id_to_Options' not in st.session_state:
        st.session_state['aoi_op_id_to_Options'] = []

    if 'aoi_create_time_to_Options' not in st.session_state:
        st.session_state['aoi_create_time_to_Options'] = []

    if 'sheet_id_to_Options' not in st.session_state:
        st.session_state['sheet_id_to_Options'] = []

    if 'ins_type_to_Options' not in st.session_state:
        st.session_state['ins_type_to_Options'] = []

    if 'df_lum' not in st.session_state:
        st.session_state['df_lum'] = pd.DataFrame()

    if 'df_aoi' not in st.session_state:
        st.session_state['df_aoi'] = pd.DataFrame()

    if 'fs' not in st.session_state:
        st.session_state['fs'] = None

    if 'MLAD' not in st.session_state:
        st.session_state['MLAD'] = None

    if 'df_result3' not in st.session_state:
        st.session_state['df_result3'] = pd.DataFrame()

    if 'df_lum_combine_data_result' not in st.session_state:
        st.session_state['df_lum_combine_data_result'] = pd.DataFrame()

    if 'df_result4' not in st.session_state:
        st.session_state['df_result4'] = pd.DataFrame()

    if 'MAP_col' not in st.session_state:
        st.session_state['MAP_col'] = []
        
    if 'TNABO_MAP' not in st.session_state:
        st.session_state['TNABO_MAP'] = []
        
    if 'TRADE_MAP' not in st.session_state:
        st.session_state['TRADE_MAP'] = []
        
    if 'TRARE_MAP' not in st.session_state:
        st.session_state['TRARE_MAP'] = []

    if 'df_result4_MAP' not in st.session_state:
        st.session_state['df_result4_MAP'] = []


def get_analysis_basic_df(success_cnt_ls:list, failed_cnt_ls:list, added_cnt_ls:list) -> pd.DataFrame:
    """Get the initial analysis format dataframe

    Params:
    -------
        success_cnt_ls (list): success cnt series of RGB
        failed_cnt_ls (list): faild cnt series of RGB
        added_cnt_ls (list): added cnt series of RGB

    Returns:
        pd.DataFrame: basic information of dataframe
        
    >>> success_cnt_ls = [3,4,5,6]
    >>> failed_cnt_ls = [2,1,1,0]
    >>> added_cnt_ls = [4,5,6,7]
    >>> df = get_analysis_basic_df(success_cnt_ls, failed_cnt_ls, added_cnt_ls)
    >>> df
    LED_TYPE 成功 失敗 新增
    B        3    2    4
    G        4    1    5
    R        5    1    6
    總計      6    1    7
    
    """
    assert len(success_cnt_ls) == 4, 'length of success_cnt_ls should be equal to 4'
    assert len(failed_cnt_ls) == 4, 'length of failed_cnt_ls should be equal to 4'
    assert len(added_cnt_ls) == 4, 'length of added_cnt_ls should be equal to 4'
    
    analysis_format = {
        'LED_TYPE' : ['B', 'G', 'R', '總計'],
        '成功' : success_cnt_ls,
        '失敗' : failed_cnt_ls,
        '新增' : added_cnt_ls,
    }
    df = pd.DataFrame(analysis_format)
    return df


def get_BGR_info(df:pd.DataFrame, light_on_OPID:str) -> pd.Series:
    LED_TYPE_ls = ['B', 'G', 'R']
    cnt_each_type_ls = []
    
    for LED_TYPE in LED_TYPE_ls:
        led_type_df = df[df['LED_TYPE']==LED_TYPE]
        dark_point_cnt = len(led_type_df[light_on_OPID].tolist())
        cnt_each_type_ls.append(dark_point_cnt)
        
    total = sum(cnt_each_type_ls)
    cnt_each_type_ls.append(total)
    cnt_each_type_series = pd.Series(cnt_each_type_ls)
    return cnt_each_type_series


def get_AOI_LUM_info(df:pd.DataFrame, LUM_OPID:str, AOI_OPID:str) -> pd.DataFrame:
    info_df = pd.pivot_table(df[df[LUM_OPID] != ''], values='CK', index=['LED_TYPE'], columns=[AOI_OPID], aggfunc=np.sum)
    info_df = info_df.reset_index()
    info_df = info_df.fillna(0)
    info_df['總暗點數'] = 0

    # 加總每個原因的個數並在每個 AOI的 cols 最底下加上 row name(總計)
    df[AOI_OPID].fillna('', inplace=True)
    col_list = list(dict.fromkeys(df[AOI_OPID].tolist()))
    items = np.delete(pd.Series(col_list), np.where(pd.Series(col_list)==''))
    
    for k in items:
        info_df['總暗點數'] += info_df[k]
    sum_list = ['總計']
    
    for pivot_col in info_df.columns:
        if pivot_col != 'LED_TYPE':
            sum_list.append(sum(info_df[pivot_col]))
    
    info_df.loc[len(info_df.index)] = sum_list   
    return info_df


def set_bg_hack_url(image_url=None) -> None:
    '''
    A function to unpack an image from url and set as bg.
    Returns
    -------
    The background.
    '''
        
    st.markdown(
         f"""
         <style>
         .stApp {{
             background: url({image_url});
             background-size: cover
         }}
         </style>
         """,
         unsafe_allow_html=True
    )


def create_hyper_link_button(text:str, link:str) -> None:
    """The function create a button object from st.markdown that insert a hyper link

    Args:
        text (str): Text of Button
        link (str): hyper link URL
    """
    st.markdown(
            f'''
            <a href={link}>
            <button style="background-color:GreenYellow;"><font size="4">
            {text}
            </font><br></button><br><br></a>
            ''',
            unsafe_allow_html=True
        )


init()


dark_point = 'LED 亮/暗(輝度)'
AB06 = '缺晶/Not Found'
light_on_his_sheet = 'light_on_History'

config_file = open("./config.json", "rb")
config = json.load(config_file)


with st.sidebar:
    with st.spinner("Loading..."):
        st.write("點擊以下連結進入其他系統")
        
        MT_analysis_url = 'http://ltwma01:8501/'
        TFTxCOC2_url = 'http://10.88.19.29:8048'
        
        create_hyper_link_button('Light_on & AOI 跨站分析系統', MT_analysis_url)
        create_hyper_link_button('TFT & COC2 比對系統', TFTxCOC2_url)



with st.container():
    set_bg_hack_url()
    with st.form(key='form1'):
        sheet_id = st.text_input(label='輸入SHEET_ID', placeholder='SHEET_ID')

        ins_type = st.multiselect(label='選擇檢測條件', options=['L255', 'L0', 'L10'])

        submitted_search_by_sheet_id = st.form_submit_button(label='確認搜尋條件')
        if submitted_search_by_sheet_id:
            if sheet_id == '' or ins_type == []:
                st.error('請輸入SHEET_ID或選擇檢測條件')
            else:
                with st.spinner('Wait for it...'):
                    MLAD = Merge_LUM_and_AOI_Defect(SHEET_ID=sheet_id, key=ins_type)
                    st.session_state['MLAD'] = MLAD
                    
                    # LUM資料庫
                    df_lum = st.session_state['MLAD'].search_NG_LUM_InsType_dataframe()
                    st.session_state['df_lum'] = df_lum
                    
                    # AOI資料庫
                    df_aoi = st.session_state['MLAD'].get_NG_TFT_AOI_dataframe()
                    st.session_state['df_aoi'] = df_aoi

                    if df_lum.empty:
                        st.error('LUM SHEET_ID不存在，請重新輸入')
                        
                    elif df_aoi.empty:
                        st.error('找不到AOI資料')
                        
                    else:
                        # 取得LUM df的 OPID 和 CreateTime 的選單
                        lum_op_id_to_Options = list(dict.fromkeys(df_lum['OPID'].tolist()))
                        st.session_state['lum_op_id_to_Options'] = lum_op_id_to_Options

                        lum_create_time_to_Options = list(dict.fromkeys(df_lum['CreateTime'].tolist()))
                        st.session_state['lum_create_time_to_Options'] = lum_create_time_to_Options

                        # 取得AOI df的 OPID 和 CreateTime 的選單
                        aoi_op_id_to_Options = list(dict.fromkeys(df_aoi['OPID'].tolist()))
                        st.session_state['aoi_op_id_to_Options'] = aoi_op_id_to_Options

                        aoi_create_time_to_Options = list(dict.fromkeys(df_aoi['CreateTime'].tolist()))
                        st.session_state['aoi_create_time_to_Options'] = aoi_create_time_to_Options

                        st.success('提交成功, 搜尋 SHEET_ID: ' + str(sheet_id) + ', 檢查條件: ' + str(ins_type))


    if st.session_state['df_lum'].empty:
        pass
    
    else:
        condition_col1, condition_col2 = st.columns(2)
        
        with condition_col1:
            OPID_lum = st.multiselect(label='選擇要查詢的LUM站點', options=st.session_state['lum_op_id_to_Options'], key='OPID_LUM')

            df_lum_col = st.session_state['df_lum'].columns.tolist()
            df_lum_result = pd.DataFrame(columns=df_lum_col)
            
            for i in range(len(OPID_lum)):
                df_lum_result = pd.concat([df_lum_result, st.session_state['df_lum'][st.session_state['df_lum'].OPID==OPID_lum[i]]])

            df_lum_result['CreateTime'] = df_lum_result['CreateTime'] + ' (' + df_lum_result['OPID'] + ')'
            CreateTime_lum = st.multiselect(label='選擇要查詢的LUM時間', options=list(dict.fromkeys(df_lum_result['CreateTime'].tolist())), key='CreateTime_LUM')
            
        with condition_col2:
            OPID_aoi = st.multiselect(label='選擇要查詢的AOI站點', options=st.session_state['aoi_op_id_to_Options'], key='OPID_AOI')

            df_aoi_col = st.session_state['df_aoi'].columns.tolist()
            df_aoi_result = pd.DataFrame(columns=df_aoi_col)
            
            for i in range(len(OPID_aoi)):
                df_aoi_result = pd.concat([df_aoi_result, st.session_state['df_aoi'][st.session_state['df_aoi'].OPID==OPID_aoi[i]]])
                
            df_aoi_result['CreateTime'] = df_aoi_result['CreateTime'] + ' (' + df_aoi_result['OPID'] + ')'
            CreateTime_aoi = st.multiselect(label='選擇要查詢的AOI時間', options=list(dict.fromkeys(df_aoi_result['CreateTime'].tolist())), key='CreateTime_AOI')
    
        col1, col2 = st.columns(2)
        
        with col1:
            submitted_combine_data = st.button(label="搜尋")
            
        with col2:
            submitted_excel = st.button(label="轉換成EXCEL")
            
            if submitted_excel:
                if st.session_state['df_lum_combine_data_result'].empty:
                    st.error('沒有資料')
                else:
                    with st.spinner('Wait for it...'):
                        
                        # buffer to use for excel writer
                        buffer = BytesIO()
                        df = st.session_state['df_lum_combine_data_result']
                        
                        # 因為插到excel要變成圖片所以要把連結文字清除
                        for col in st.session_state['MAP_col']:
                            df[col] = ''
                        df_col_num = len(df.columns)
                        
                        # download button 2 to download dataframe as xlsx
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            # Write each dataframe to a different worksheet.
                            df.to_excel(writer, sheet_name='Sheet1', index=False)

                            # Get the xlsxwriter workbook and worksheet objects.
                            workbook = writer.book

                            worksheet = writer.sheets['Sheet1']
                            cell_format = workbook.add_format({'align': 'center'})
                            
                            map_col_index = []
                            for col in st.session_state['MAP_col']:
                                map_col_num = df.columns.tolist().index(col)
                                
                                # 紀錄MAP的col index
                                map_col_index.append(map_col_num)
                                
                                # TNABO_MAP, TRADE_MAP, TRARE_MAP
                                for i in range(len(st.session_state[col])):
                                    url = st.session_state[col][i]
                                    worksheet.set_row(i+1, 100)
                                    
                                    try:
                                        image_data = BytesIO(urlopen(url).read())
                                        # Insert an image.
                                        worksheet.insert_image(f'{chr(65+map_col_num)}{str(i+2)}', url, {'image_data': image_data, 'url': url, 'x_scale': 0.5, 'y_scale': 0.5, 'object_position': 1})
                                    except:
                                        worksheet.write(f'{chr(65+map_col_num)}{str(i+2)}', url)
                                        pass

                            # 設定全部的 col 寬度
                            worksheet.set_column(f'{chr(65)}:{chr(65+df_col_num)}', 15, cell_format)
                            # 設定個別的 MAP col 寬度
                            for col_index in map_col_index:
                                worksheet.set_column(f'{chr(65+col_index)}:{chr(65+col_index)}', 25, cell_format)
                            
                            # 要做autofit的格子
                            # worksheet.autofit()
                            # worksheet.set_column(f'{chr(65+map_col_num)}:{chr(65+map_col_num)}', 25, cell_format)
                            
                            # 建立單站的 Summary
                            st.session_state['df_lum_combine_data_result']['CK'] = 1
                            
                            aoi_summary_ls = ['ABO', 'ADE', 'ARE']
                            all_cols = df.columns.tolist()
                            
                            for i in range(7, len(all_cols)):
                                if '-' in all_cols[i] or '+' in all_cols[i]:
                                   aoi_summary_ls = ['-ACO', '+ACO', '+ACO2']
                                break
                            
                            
                            decnt = 0
                            recnt = 0
                            cols = df.columns.tolist()
                            light_on_cols = []
                            for j in range(len(cols)):
                                # 用來製作 Light on history 
                                if 'L' in cols[j] and cols[j] != 'LED_TYPE':
                                    light_on_cols.append(cols[j])
                                    
                                for aoi_opid in aoi_summary_ls:
                                    if cols[j].endswith(aoi_opid):
                                        table_temp = pd.pivot_table(
                                            
                                            data=st.session_state['df_lum_combine_data_result'], 
                                            values='CK', index=['LED_TYPE'], columns=[cols[j]], aggfunc=np.sum
                                        )
                                        
                                        table_temp = table_temp.reset_index()
                                        table_temp = table_temp.fillna(0)
                                        table_temp['總計'] = 0
                           
                                        # 加總每個原因的個數並在每個 AOI的 cols 最底下加上 row name(總計)
                                        col_list = list(dict.fromkeys(st.session_state['df_lum_combine_data_result'][cols[j]]))
                                        for i in col_list:
                                            table_temp['總計'] += table_temp[i]
                                        sum_list = ['總計']
                                        
                                        for pivot_col in table_temp.columns:
                                            if pivot_col != 'LED_TYPE':
                                                sum_list.append(sum(table_temp[pivot_col]))
                                                
                                        table_temp.loc[len(table_temp.index)] = sum_list
                                        
                                        origin_df = st.session_state['df_lum_combine_data_result']
                                        
                                        # AB10 不計算
                                        # filter_AB10_df = origin_df
                                        
                                        # 以 暗點 為出發點
                                        # 計算 成功(前一站->當站) 新增(前一站->當站) 失敗(前一站->當站)   
                                        
                                        if aoi_opid in config['AOI_OPID_list']['De_Bond_list']:
                                            RGB_total_cnt_ls = get_BGR_info(
                                                origin_df[(origin_df[cols[j-1]] == dark_point)], cols[j-1]
                                            )
                                            
                                            info = get_AOI_LUM_info(origin_df, LUM_OPID=cols[j-1], AOI_OPID=cols[j])
                                            
                                            
                                            # 1. 成功 (暗 -> 暗)
                                            succcess_df = origin_df[
                                                # light in
                                                (origin_df[cols[j-4]] == dark_point) & 
                                                # AOI
                                                (origin_df[cols[j-3]] == 'LED已上件') & 
                                                # light in
                                                (origin_df[cols[j-1]] == dark_point) & 
                                                # AOI
                                                (origin_df[cols[j]] == AB06) 
                                            ]
                                            RGB_success_cnt_ls = get_BGR_info(succcess_df, cols[j-1])
                                            del succcess_df
                                            
                                            # 2. 新增 (空白->暗 (暗點))
                                            added_dot_df = origin_df[
                                                (origin_df[cols[j-4]] == '') & 
                                                (origin_df[cols[j-1]] == dark_point)
                                            ]
                                            RGB_added_cnt_ls = get_BGR_info(added_dot_df, cols[j-1])
                                            del added_dot_df
                                            
                                            # 3. 失敗 (有LED -> 有LED)
                                            failed_df = origin_df[
                                                (origin_df[cols[j-4]] == dark_point) & 
                                                (origin_df[cols[j-3]] == 'LED已上件') & 
                                                (origin_df[cols[j-1]] == dark_point) & 
                                                (origin_df[cols[j-1]] == 'LED已上件')
                                            ]
                                            
                                            RGB_failed_cnt_ls = get_BGR_info(failed_df, cols[j-1])
                                            del failed_df
                                        
                                            
                                            De_info = get_analysis_basic_df(
                                                success_cnt_ls=RGB_success_cnt_ls,
                                                failed_cnt_ls=RGB_failed_cnt_ls,
                                                added_cnt_ls=RGB_added_cnt_ls
                                            )
                                            
                                            De_info['Debond成功率'] = (De_info['成功']/(De_info['成功'] + De_info['失敗']))*100
                                            De_info = info.merge(De_info, on='LED_TYPE', how='right')
                                            De_info.fillna(0, inplace=True)
                                            
                                            table_temp.insert(0, f"{cols[j]} AOI Info", "")
                                            De_info.insert(0, f"{cols[j-1]} Light on Info", "")
                                            De_info.insert(De_info.columns.to_list().index('總暗點數'), "", "")
                                            
                                            debond_sheet = 'Debond_summary'
                                            if debond_sheet not in workbook.sheetnames.keys():
                                                table_temp.to_excel(
                                                    writer, debond_sheet, index=False
                                                )
                                                
                                                De_info.to_excel(
                                                    writer, debond_sheet, index=False, startrow=6
                                                )
                                                
                                            else:
                                                table_temp.to_excel(
                                                    writer, debond_sheet, index=False, startrow=11 + int((j/4))
                                                )
                                                
                                                De_info.to_excel(
                                                    writer, debond_sheet, index=False, startrow=17 + int((j/4))
                                                )
                                            
                                            # insert df to light on history
                                            if light_on_his_sheet not in workbook.sheetnames.keys():
                                                De_info.to_excel(
                                                    writer, light_on_his_sheet, index=False, startrow=j
                                                )
                                                
                                            else:
                                                De_info.to_excel(
                                                    writer, light_on_his_sheet, index=False, startrow=j+(decnt*10)
                                                )
                                                
                                            decnt += 1
                                        
                                                    
                                        elif aoi_opid in config['AOI_OPID_list']['Repair_list']:
                                            # 計算 成功 (暗->亮) 新增(空白->暗) 失敗(暗->暗)
                                            RGB_total_cnt_ls = get_BGR_info(
                                                origin_df[(origin_df[cols[j-1]] == dark_point)], cols[j-1]
                                            )
                                            
                                            info = get_AOI_LUM_info(origin_df, LUM_OPID=cols[j-1], AOI_OPID=cols[j])
                                            
                                            
                                            # 1. 成功 (暗->亮 (無暗點))
                                            succcess_df = origin_df[
                                                (origin_df[cols[j-4]] == dark_point) & 
                                                (origin_df[cols[j-1]] == '')
                                            ]
                                            RGB_success_cnt_ls = get_BGR_info(succcess_df, cols[j-1])
                                            del succcess_df
                                            
                                            # 2. 新增 (空白->暗 (有暗點))
                                            added_dot_df = origin_df[
                                                (origin_df[cols[j-4]] == '') & 
                                                (origin_df[cols[j-1]] == dark_point)
                                            ]
                                            RGB_added_cnt_ls = get_BGR_info(added_dot_df, cols[j-1])
                                            del added_dot_df
                                            
                                            # 3. 失敗 (暗 (有暗點)->暗 (有暗點))
                                            failed_df = origin_df[
                                                (origin_df[cols[j-4]] == dark_point) & 
                                                (origin_df[cols[j-1]] == dark_point)
                                            ]
                                        
                                            RGB_failed_cnt_ls = get_BGR_info(failed_df, cols[j-4])
                                            del failed_df
                                                
                                            
                                            Re_info = get_analysis_basic_df(
                                                success_cnt_ls=RGB_success_cnt_ls,
                                                failed_cnt_ls=RGB_failed_cnt_ls,
                                                added_cnt_ls=RGB_added_cnt_ls
                                            )
                                            
                                            Re_info['Repair成功率'] = (Re_info['成功']/(Re_info['成功']+Re_info['失敗']))*100
                                            Re_info = info.merge(Re_info, on='LED_TYPE', how='right')
                                            Re_info.fillna(0, inplace=True)
                                            
                                            table_temp.insert(0, f"{cols[j]} AOI Info", '')
                                            Re_info.insert(0, f"{cols[j-1]} Light on Info", '')
                                            Re_info.insert(Re_info.columns.to_list().index('總暗點數'), "", "")
                                            
                                            repair_sheet = 'Repair_summary'
                                            if repair_sheet not in workbook.sheetnames.keys():
                                                table_temp.to_excel(
                                                    writer, repair_sheet, index=False
                                                )
                                                
                                                Re_info.to_excel(
                                                    writer, repair_sheet, index=False, startrow=6
                                                )
                                                
                                            else:
                                                table_temp.to_excel(
                                                    writer, repair_sheet, index=False, startrow=11+int(((j-1)/4)-1)
                                                )
                                                
                                                Re_info.to_excel(
                                                    writer, repair_sheet, index=False, startrow=17+int(((j-1)/4)-1)
                                                )
                                            
                                            # insert df to light on history
                                            if light_on_his_sheet not in workbook.sheetnames.keys():
                                                Re_info.to_excel(
                                                    writer, light_on_his_sheet, index=False, startrow=j+4
                                                )
                                                
                                            else:
                                                Re_info.to_excel(
                                                    writer, light_on_his_sheet, index=False, startrow=j+4+(recnt*10)
                                                )
                                                   
                                            recnt += 1
                                        
                                           
                                        elif aoi_opid in config['AOI_OPID_list']['Main_Bond_list']:
                                            RGB_total_cnt_ls = get_BGR_info(
                                                origin_df[(origin_df[cols[j-1]] == dark_point)], cols[j-1]
                                            )
                                            
                                            MB_info = get_AOI_LUM_info(origin_df, LUM_OPID=cols[j-1], AOI_OPID=cols[j])
                                   
                                            table_temp.insert(0, f"{cols[j]} AOI Info", '')
                                            MB_info.insert(0, f"{cols[j-1]} Light on Info", '')
                                            MB_info.insert(MB_info.columns.to_list().index('總暗點數'), "", "")
                                            
                                            
                                            if light_on_his_sheet not in workbook.sheetnames.keys():
                                                MB_info.to_excel(writer, light_on_his_sheet, index=False)
                                            
                                            
                                            mainbond_sheet = 'Mainbond_summary'

                                            # insert df to light on history
                                            if mainbond_sheet not in workbook.sheetnames.keys():
                                                table_temp.to_excel(writer, mainbond_sheet, index=False)
                                                MB_info.to_excel(writer, mainbond_sheet, index=False, startrow=6)
                                                
                                            else:
                                                table_temp.to_excel(
                                                    writer, mainbond_sheet, index=False, startrow=11 + int((j/3))
                                                )
                                                
                                                MB_info.to_excel(
                                                    writer, mainbond_sheet, index=False, startrow=17 + int((j/3))
                                                )
                            
                            
                            for worksheet in workbook.sheetnames.keys():
                                sheet = writer.sheets[worksheet]
                                sheet.autofit()
                            writer.close()

                            download2 = st.download_button(
                                label="Download data as Excel",
                                data=buffer,
                                file_name=f'{sheet_id}_{str(ins_type)}_{str(OPID_lum)}_{str(CreateTime_lum)}.xlsx',
                                mime='application/vnd.ms-excel'
                            )
                            st.session_state['df_lum_combine_data_result'] = pd.DataFrame()
                            

        if submitted_combine_data:
            # error_flag = False
            # for opid in OPID_aoi:
            #     cnt = 0
            #     for time in CreateTime_aoi:
            #         if opid in time:
            #             cnt += 1
            #     if cnt > 1:
            #         error_flag = True
            #         break
                
            if OPID_lum == [] or CreateTime_lum == []:
                st.error('請選擇欲查詢的 LUM 站點/時間')
            elif OPID_aoi == [] or CreateTime_aoi == []:
                st.error('請選擇欲查詢的 AOI 站點/時間')
            # elif error_flag:
            #     st.error('同一 AOI 站點時間不可重複')
            else:
                with st.spinner('Searching and Processing...'):
                    df_lum_combine_data_col = df_lum_result.columns.tolist()
                    df_lum_combine_data_result = pd.DataFrame(columns=df_lum_combine_data_col)

                    # 取得在OPID, CreateTime指定條件下的LUM資料
                    CreateTime_lum_slice = [i.split()[0] for i in CreateTime_lum]
                    df_lum_defect = st.session_state['MLAD'].search_NG_LUM_defect_dataframe(OPID=OPID_lum, CreateTime=CreateTime_lum_slice)
                    
                    # 取得在OPID, CreateTime指定條件下的AOI資料
                    CreateTime_aoi_slice = [int(i.split()[0]) for i in CreateTime_aoi]
                    df_aoi_defect = st.session_state['MLAD'].search_NG_TFT_AOI_dataframe(OPID=OPID_aoi, CreateTime=CreateTime_aoi_slice)
                    
                    # 比對AOI資料跟LUM的時間
                    df_filter_defect = st.session_state['MLAD'].compare_createTime(df_aoi_defect)
                    
                    # 結合LUM與AOI的df
                    df_lum_combine_data_result = st.session_state['MLAD'].merge_LUM_AOI_dataframe(df_lum=df_lum_defect, df_aoi=df_filter_defect)
                    
                    
                    if df_lum_combine_data_result.empty:
                        st.error('沒有資料')
                        
                    else:
                        # 用網頁顯示 dataframe
                        def show_MAP_from_url(MAP_url):
                            return(f'''<a href='{MAP_url}'><img src='{MAP_url}'></a>''')
                        
                        MAP_col = []
                        for col in df_lum_combine_data_result.columns.tolist():
                            if col.endswith('_MAP'):
                                MAP_col.append(col)
                                st.session_state[col] = df_lum_combine_data_result[col].tolist()
                                df_lum_combine_data_result[col] = df_lum_combine_data_result.apply(lambda x: show_MAP_from_url(x[col]), axis=1)
                                
                        st.session_state['MAP_col'] = MAP_col
                        final_table = df_lum_combine_data_result.reset_index(drop=True)
                        st.session_state['df_lum_combine_data_result'] = final_table

                        st.success('搜尋完成')

                        #show dataframe with MAP
                        df_html_str = final_table[:200].to_html(escape=False)
                        st.markdown(df_html_str[:35] + df_html_str[35:54] + df_html_str[81:], unsafe_allow_html=True)
