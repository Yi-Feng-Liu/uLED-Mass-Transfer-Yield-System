import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
# from connect_mongodb import connect_mongodb
from process_AOI_defect import Merge_LUM_and_AOI_Defect

st.set_page_config(
    page_title="MT",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

# search_mode = st.radio(label='請選擇搜尋模式', options=('依照SHEET_ID', ''))

# if search_mode == '依照SHEET_ID':
with st.container():
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
                            workbook  = writer.book

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
                                        del image_data
                                        
                                    except HTTPError as e:
                                        worksheet.write(f'{chr(65+map_col_num)}{str(i+2)}', url)
                                        pass
                                    
                                    except URLError as e:
                                        worksheet.write(f'{chr(65+map_col_num)}{str(i+2)}', url)
                                        pass
                                    
                                    except ValueError as e:
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
                            LED_TYPE_ls = ['R', 'G', 'B']
                            for df_col in df.columns.tolist():
                                for aoi_opid in aoi_summary_ls:
                                    if df_col.endswith(aoi_opid):
                                        table_temp = pd.pivot_table(st.session_state['df_lum_combine_data_result'], values='CK', index=['LED_TYPE'], columns=[df_col], aggfunc=np.sum)
                                        table_temp = table_temp.reset_index()
                                        table_temp = table_temp.fillna(0)
                                        table_temp['總計'] = 0
                           
                                        # 加總每個原因的個數並在每個 AOI的 col 最底下加上 row name(總計)
                                        col_list = list(dict.fromkeys(st.session_state['df_lum_combine_data_result'][df_col]))
                                        for i in col_list:
                                            table_temp['總計'] += table_temp[i]
                                        sum_list = ['總計']
                                        
                                        for pivot_col in table_temp.columns:
                                            if pivot_col != 'LED_TYPE':
                                                sum_list.append(sum(table_temp[pivot_col]))
                                                
                                        table_temp.loc[len(table_temp.index)] = sum_list
                                        
                                        filter_AB10_df = st.session_state['df_lum_combine_data_result']
                                        
                                        # AB10 不計算
                                        if 'TNABO' in filter_AB10_df.columns.tolist():
                                            filter_AB10_df = filter_AB10_df[filter_AB10_df['TNABO'] != 'Rotate']
                                            
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
                                        
                                        # 計算 成功(前一站->當站) 新增(前一站->當站) 失敗(前一站->當站)   
                                        dark_point = 'LED 亮/暗(輝度)'
                                        if aoi_opid == 'ADE':
                                            # 需扣除兩站都為 LED已上件 的數量
                                            # 扣除不用repair的才是真正要處理的
                                            # both_LED_df = filter_AB10_df[
                                            #     (filter_AB10_df['TNABO'] == '缺晶/Not Found') & (filter_AB10_df['TRADE'] == '缺晶/Not Found')
                                            # ]
                                            # both_LED_cnt = len(both_LED_df['TRLRE'].tolist())
                                            # del both_LED_df
                                            
                                            # 0. Total
                                            DE_light_on_dark_point_cnt = len(filter_AB10_df[(filter_AB10_df['TRLDE'] == dark_point)].index)
                                            
                                            RGB_total_cnt_ls = get_BGR_info(filter_AB10_df[(filter_AB10_df['TRLDE'] == dark_point)], 'TRLDE')
                                            
                                            if 'TNLBO' in filter_AB10_df.columns.tolist():
                                                BO_light_on_dark_point_cnt = len(filter_AB10_df[(filter_AB10_df['TNLBO'] == dark_point)].index)
                                                # 1. 成功 (暗 -> 暗)
                                                succcess_df = filter_AB10_df[
                                                    (filter_AB10_df['TNLBO'] == dark_point) & (filter_AB10_df['TNABO'] == 'LED已上件') & (filter_AB10_df['TRLDE'] == dark_point) & (filter_AB10_df['TRADE'] != 'LED已上件') 
                                                ]
                                                RGB_success_cnt_ls = get_BGR_info(succcess_df, 'TRLDE')
                                                del succcess_df
                                                
                                                # 2. 新增 (空白->暗 (有暗點))
                                                added_dot_df = filter_AB10_df[
                                                    (filter_AB10_df['TNLBO'] == '') & (filter_AB10_df['TRLDE'] == dark_point)
                                                ]
                                                RGB_added_cnt_ls = get_BGR_info(added_dot_df, 'TRLDE')
                                                del added_dot_df
                                                
                                                # 3. 失敗 (有LED -> 有LED)
                                                failed_df = filter_AB10_df[
                                                    (filter_AB10_df['TNLBO'] == dark_point) & (filter_AB10_df['TNABO'] == 'LED已上件') & (filter_AB10_df['TRLDE'] == dark_point) & (filter_AB10_df['TRADE'] == 'LED已上件')
                                                ]
                                                
                                                RGB_failed_cnt_ls = get_BGR_info(failed_df, 'TRLDE')
                                                del failed_df
                                                
                                            else:
                                                RGB_success_cnt_ls, RGB_added_cnt_ls, RGB_failed_cnt_ls = 0, 0, 0
                                                
                                            debond_info = pd.DataFrame()
                                            debond_info['LED_TYPE'] = table_temp['LED_TYPE']
                                            debond_info['總暗點數'] = RGB_total_cnt_ls
                                            debond_info['成功'] = RGB_success_cnt_ls
                                            debond_info['失敗'] = RGB_failed_cnt_ls
                                            debond_info['新增'] = RGB_added_cnt_ls
                                            debond_info['Debond成功率'] = (debond_info['成功']/(debond_info['成功'] + debond_info['失敗']))*100
                                            merge_table_temp = table_temp.merge(debond_info, how='right', on='LED_TYPE')
                                    
                                        elif aoi_opid == 'ARE':
                                            # 以 Repair 為出發點
                                            # 計算 成功 (暗->亮) 新增(空白->暗) 失敗(暗->暗)
                                            
                                            # 0. Total
                                            RE_light_on_dark_point_cnt = len(filter_AB10_df[(filter_AB10_df['TRLRE'] == dark_point)].index)
                                            RGB_total_cnt_ls = get_BGR_info(filter_AB10_df[(filter_AB10_df['TRLRE'] == dark_point)], 'TRLRE')
                                            
                                            # # 需扣除兩站都為 LED已上件 的數量
                                            # # 扣除不用repair的才是真正要處理的
                                            # both_LED_df = filter_AB10_df[
                                            #     (filter_AB10_df['TRADE'] == 'LED已上件') & (filter_AB10_df['TRARE'] == 'LED已上件')
                                            # ]
                                            # both_LED_cnt = len(both_LED_df['TRLRE'].tolist())
                                            # del both_LED_df
                                            
                                            # actual_process_cnt = RE_light_on_dark_point_cnt - both_LED_cnt
                                            
                                            if 'TRLDE' in filter_AB10_df.columns.tolist():
                                                DE_light_on_dark_point_cnt = len(filter_AB10_df[(filter_AB10_df['TRLDE'] == dark_point)].index)
                                                # 1. 成功 (暗->亮 (無暗點))
                                                succcess_df = filter_AB10_df[
                                                    (filter_AB10_df['TRLDE'] == dark_point) & (filter_AB10_df['TRLRE'] == '')
                                                ]
                                                RGB_success_cnt_ls = get_BGR_info(succcess_df, 'TRLRE')
                                                del succcess_df
                                                
                                                # 2. 新增 (空白->暗 (有暗點))
                                                added_dot_df = filter_AB10_df[(filter_AB10_df['TRLDE'] == '') & (filter_AB10_df['TRLRE'] == dark_point)]
                                                RGB_added_cnt_ls = get_BGR_info(added_dot_df, 'TRLRE')
                                                del added_dot_df
                                                
                                                # 3. 失敗 (暗 (有暗點)->暗 (有暗點))
                                                failed_df = filter_AB10_df[(filter_AB10_df['TRLDE'] == dark_point) & (filter_AB10_df['TRLRE'] == dark_point)]
                                            
                                                RGB_failed_cnt_ls = get_BGR_info(failed_df, 'TRLDE')
                                                del failed_df
                                            else:
                                                RGB_success_cnt_ls, RGB_added_cnt_ls, RGB_failed_cnt_ls = 0, 0, 0
                                                
                                            repair_info = pd.DataFrame()
                                            repair_info['LED_TYPE'] = table_temp['LED_TYPE']
                                            repair_info['總暗點數'] = RGB_total_cnt_ls
                                            repair_info['成功'] = RGB_success_cnt_ls
                                            repair_info['失敗'] = RGB_failed_cnt_ls
                                            repair_info['新增'] = RGB_added_cnt_ls
                                            repair_info['Repair成功率'] = (repair_info['成功']/(repair_info['成功']+repair_info['失敗']))*100
                                            merge_table_temp = table_temp.merge(repair_info, how='right', on='LED_TYPE')
                      
                                        else:  
                                            RGB_total_cnt_ls = get_BGR_info(filter_AB10_df[(filter_AB10_df['TNLBO'] == dark_point)], 'TNLBO')  
                                            MB_info = pd.DataFrame()
                                            MB_info['LED_TYPE'] = table_temp['LED_TYPE']
                                            MB_info['總暗點數'] = RGB_total_cnt_ls
                                            merge_table_temp = table_temp.merge(MB_info, how='right', on='LED_TYPE')

                                        merge_table_temp.insert(0, "AOI Info", '')
                                        spec_col_index = merge_table_temp.columns.tolist().index('總計')
                                        merge_table_temp.insert(spec_col_index + 1, "Light-on Info", '')
                                        merge_table_temp.to_excel(writer, sheet_name=df_col + '_summary', index=False)
                                        
                            # Close the Pandas Excel writer and output the Excel file to the buffer
                            writer.close()

                            download2 = st.download_button(
                                label="Download data as Excel",
                                data=buffer,
                                file_name=f'{sheet_id}_{str(ins_type)}_{str(OPID_lum)}_{str(CreateTime_lum)}.xlsx',
                                mime='application/vnd.ms-excel'
                            )
                            st.session_state['df_lum_combine_data_result'] = pd.DataFrame()
                            

        if submitted_combine_data:
            error_flag = False
            for opid in OPID_aoi:
                cnt = 0
                for time in CreateTime_aoi:
                    if opid in time:
                        cnt += 1
                if cnt > 1:
                    error_flag = True
                    break
                
            if OPID_lum == [] or CreateTime_lum == []:
                st.error('請選擇欲查詢的 LUM 站點/時間')
            elif OPID_aoi == [] or CreateTime_aoi == []:
                st.error('請選擇欲查詢的 AOI 站點/時間')
            elif error_flag:
                st.error('同一 AOI 站點時間不可重複')
            else:
                with st.spinner('Searching...'):
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
                        # show dataframe
                        # st.dataframe(df_lum_combine_data_result)
                        
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
                        
                        # st.write(df_html_str[:35] + ' style="table-layout: fixed; width: 100%;"' + df_html_str[35:54] + df_html_str[81:])
                        st.markdown(df_html_str[:35] + df_html_str[35:54] + df_html_str[81:], unsafe_allow_html=True)
