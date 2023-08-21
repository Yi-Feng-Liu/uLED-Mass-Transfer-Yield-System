import zeep
import base64
import os
from datetime import datetime, timedelta
import paramiko
from glob import glob


class auto_mail():
    def __init__(self):
        # self.Rmail_user = 'Alan.YF.Liu@auo.com;'
        # self.Cmail_user = 'Alan.YF.Liu@auo.com;'
        self.Rmail_user = 'Alan.YF.Liu@auo.com'
        self.Cmail_user = 'Alan.YF.Liu@auo.com'
        self.now = datetime.now().strftime('%Y%m%d%H%M')
        self.client = zeep.Client("URL") 
        
        # 連到放圖片的 server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("User", 22, "wma", "wma")
        self.sftp_client = ssh.open_sftp()
        self.OUTPUT_FILE_PATH = './report_production/'

        self.key_MB_IMG_dict = {
            13.6: './report_production/MainBondYiled13.6/',
            16.1: './report_production/MainBondYiled16.1/',
            17.3: './report_production/MainBondYiled17.3/'
        }
        self.key_model_path_dict = {
            13.6: './report_production/13.6/',
            16.1: './report_production/16.1/',
            17.3: './report_production/17.3/'
        }

    def sendReport(self, filepath: str, messageForTableType: str, MODEL):
        if os.path.exists(filepath):
            filename = filepath.split("/")[-1]
            with open(filepath, "rb") as f:
                encoded_string = base64.b64encode(f.read())
                encoded_ppt_file = encoded_string.decode('utf-8')

            MFG_DATE = (datetime.now() - timedelta(hours=7.5)).strftime("%Y/%m/%d")[:10]

            MB_mail_picture_string = ''
            # read the report image and the file name endswith is jpg
            MB_PATH = self.key_MB_IMG_dict.get(MODEL)
            if len(glob(MB_PATH + "*.png")) != 0:
                for i in glob(MB_PATH+ "*.png"):
                    var_str = i.split("/")[-1]
                    self.sftp_client.put(
                        localpath=MB_PATH+ var_str, remotepath=f"/save/for/mail_image/{var_str}"
                    )
                    html_AOI_chart = '<br><br>' + MFG_DATE + '<br><br>' + f'<img src="http://user:8081/download/mail_image/{var_str}"  width="1000" height="600">'
                    MB_mail_picture_string = MB_mail_picture_string + html_AOI_chart + '<br><br>'

            mail_picture_string = ''
            # read the report image and the file name endswith is .jpg
            NotMB_PATH = self.key_model_path_dict.get(MODEL)
            if len(glob(NotMB_PATH + "*.jpg")) != 0:
                for i in glob(NotMB_PATH + "*.jpg"):
                    var_str = i.split("/")[-1]
                    self.sftp_client.put(
                        localpath=NotMB_PATH + var_str, remotepath=f"/save/for/mail_image/{var_str}"
                    )
                    html_AOI_chart = '<br><br>' + MFG_DATE + '<br><br>' + f'<img src="http://user:8081/download/mail_image/{var_str}"  width="1000" height="800">'
                    mail_picture_string = mail_picture_string + html_AOI_chart + '<br><br>'

            # Image of Main Bond will be set the first image on the mail page.
            total_picture_string = MB_mail_picture_string + mail_picture_string

            html_PPT_report_link = ''
            pptx_PATH = self.key_model_path_dict.get(MODEL)
            if len(glob(pptx_PATH + "*.pptx")) != 0:
                for j in glob(pptx_PATH + "*.pptx"):
                    var_str = j.split("/")[-1]
                    self.sftp_client.put(
                        localpath=pptx_PATH + var_str, remotepath=f"/save/for/mail_image/{var_str}"
                    )
                    html_PPT_report_link = '<br><br>' + MFG_DATE + '<br><br>' + f'<a href="http://user:8081/download/mail_image/{var_str}"><h3>PPT Report link. If you want to download the ppt report, please click here to download.</h3></a>'

            if total_picture_string != '':
                ManualSend_01 = {
                    'strMailCode': 'OOt4AZp0pAo=', #MailCode
                    'strRecipients':'' +  self.Rmail_user + '', 
                    'strCopyRecipients':'' + self.Cmail_user + '', #副本
                    'strSubject': f'({messageForTableType} information) uLED MTL REPORT at  ' + self.now , #標題
                    'strBody':
                    '<b><font color="#E00000">' + 
                    'Hi All: ' + '</font></b>' + 
                    ' At ' + self.now + ' had   INSPECT  REPORT ' + 
                    '<br><br><font color="#1500F0"><font face="Noto Sans TC">' + 
                    'Please check the PPT_REPORT.' + '<br><br>' + 
                    'Notice: ' + '<br><br>' + 
                    'The file will only be kept for one week and will be deleted when it expires.' +
                    '</font></font><br><br>' + html_PPT_report_link +
                    total_picture_string + 
                    '', 
                    'strFileBase64String': filename + ':' + str(encoded_ppt_file) +
                    ''
                }
                response_01 = self.client.service.ManualSend_39(**ManualSend_01)  
        else:
            ManualSend_01 = {
                'strMailCode': 'OOt4AZp0pAo=',
                'strRecipients':'' + self.Rmail_user + '',
                'strCopyRecipients':'' + self.Cmail_user + '',
                'strSubject': f'({messageForTableType} information) uLED MTL NO LUM DATA at {self.now}',
                'strBody':
                '<b><font color="#E00000">' + 
                'Hi All: ' + '</font></b>' + 
                '<br><br><font face="Noto Sans TC"> NO LUM DATA at ' + self.now + '</font><br><br>'
                ''
            }
            response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send report mail successful or not: ' + str(response_01))



# class alarmAutoMail(auto_mail):
#     def __call__(self, filePath:str, ModelNo:str) -> None:
#         self.send(filePath, ModelNo)
        
        
#     def send(self, filePath, ModelNo):
#         self.Rmail_user = 'Alan.YF.Liu@auo.com'
#         self.Cmail_user = 'Alan.YF.Liu@auo.com'
#         ManualSend_01 = {
#                 'strMailCode': 'OOt4AZp0pAo=',
#                 'strRecipients': '' + self.Rmail_user + '', 
#                 'strCopyRecipients': '' + self.Cmail_user + '', 
#                 'strSubject': 'MODEL_NO Warning at ' + self.now ,
#                 'strBody': '<b><font color="#E00000">' + 
#                 'Hi All: ' + '</font></b>' + 
#                 '<br><br><font face="Noto Sans TC">' + 
#                 '[Warning]' + '<br><br>' + 
#                 'File: ' + '<font color="#1500F0">' + filePath + '</font></font><br><br>' +
#                 '<font face="Noto Sans TC">' + 
#                 'The MODEL_NO' + '<font color="#1500F0">' + ModelNo + ' was not in PRODUCT.json or cannot be recognized' + 
#                 '</font></font>' +
#                 '<font face="Noto Sans TC">' + '---- ' + self.now + '</font><br><br>'
#                 ''
#             }
#         response_01 = self.client.service.ManualSend_07(**ManualSend_01)
#         print('Send alarm mail successful or not: ' + str(response_01))


class customMessageAutoMail(auto_mail):
    """Send the specific subject and message to people

    Args:
        subject (str)
        message (str)
    """
    
    def __init__(self):
        super(customMessageAutoMail, self).__init__()
        # self.Rmail_user = 'Alan.YF.Liu@auo.com;'
        # self.Cmail_user = 'Alan.YF.Liu@auo.com;'
        self.Rmail_user = 'Alan.YF.Liu@auo.com'
        self.Cmail_user = 'Alan.YF.Liu@auo.com'
    
    
    def __call__(self, subject:str, message:str) -> None:
        self.send(subject, message)
    
     
    def send(self, subject:str, message: str):
        ManualSend_01 = {
                'strMailCode': 'OOt4AZp0pAo=',
                'strRecipients':'' + self.Rmail_user + '', 
                'strCopyRecipients':'' + self.Cmail_user + '', 
                'strSubject': f'{subject} ' + self.now , 
                'strBody':'<b><font color="#E00000">' + 
                'Hi All: ' + '</font></b>' +
                '<br><br><font face="Noto Sans TC">Please Check the File <br>' + message + 
                '<br>---- ' + self.now + '</font><br><br>'
                ''
            }
        response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send custom message mail successful or not: ' + str(response_01))


