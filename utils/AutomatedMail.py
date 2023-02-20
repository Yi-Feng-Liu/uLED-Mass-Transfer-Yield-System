import zeep
import base64
import os
from datetime import datetime, timedelta
import paramiko
from glob import glob


class auto_mail():
    def __init__(self):
        self.Rmail_user = 'Alan.YF.Liu@auo.com'
        self.Cmail_user = 'Alan.YF.Liu@auo.com'
        self.now = datetime.now().strftime('%Y%m%d%H%M')
        self.client = zeep.Client("http://ids.cdn.corpnet.auo.com/IDS_WS/Mail.asmx?wsdl") 
        # connect to the server for save image 
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("ltaiw01", 22, "wma", "wma")
        self.sftp_client = ssh.open_sftp()
        self.OUTPUT_FILE_PATH = './report_production/'

    def sendReport(self, filepath: str, messageForTableType: str):
        # the mail will be send with ppt file if ppt file exit.
        if os.path.exists(filepath):
            filename = filepath.split("/")[-1]
            with open(filepath, "rb") as f:
                encoded_string = base64.b64encode(f.read())
                encoded_ppt_file = encoded_string.decode('utf-8')

            # var_list = []
            MFG_DATE = (datetime.now() - timedelta(hours=7.5)).strftime("%Y/%m/%d")[:10]


            mail_picture_string = ''

            # read the report image and the file name endswith is .jpg
            if len(glob(self.OUTPUT_FILE_PATH + "*.jpg")) != 0:
                for i in glob(self.OUTPUT_FILE_PATH + "*.jpg"):
                    var_str = i.split("\\")[-1]
                    self.sftp_client.put(
                        localpath=self.OUTPUT_FILE_PATH + var_str, remotepath=f"/app_1/wma/deploy/download/mail_image/{var_str}"
                    )
                    html_AOI_chart = '<br><br>' + MFG_DATE + '<br><br>' + f'<img src="http://ltaiw01:8081/download/mail_image/{var_str}"  width="2100" height="1500">'

                    mail_picture_string = mail_picture_string + html_AOI_chart + '<br><br>'

                    # var_list.append(html_AOI_chart)

            html_PPT_report_link = ''
            if len(glob(self.OUTPUT_FILE_PATH + "*.pptx")) != 0:
                for j in glob(self.OUTPUT_FILE_PATH + "*.pptx"):
                    var_str = j.split("\\")[-1]
                    self.sftp_client.put(
                        localpath=self.OUTPUT_FILE_PATH + var_str, remotepath=f"/app_1/wma/deploy/download/mail_image/{var_str}"
                    )
                    html_PPT_report_link = '<br><br>' + MFG_DATE + '<br><br>' + f'<a href="http://ltaiw01:8081/download/mail_image/{var_str}"><h3>PPT Report link</h3></a>'
                    

            if len(mail_picture_string) != 0:
                # var_str_type = ','.join(var_list)

                ManualSend_01 = {
                        'strMailCode': 'OOt4AZp0pAo=', #MailCode
                        'strRecipients':'' +  self.Rmail_user + '', 
                        'strCopyRecipients':'' + self.Cmail_user + '', #副本
                        'strSubject': f'({messageForTableType} information)uLED MTL REPORT at ' + self.now , #標題
                        'strBody':
                        '<b><font color="#E00000">' + 
                        'Hi All: ' + '</font></b>' + 
                        ' At ' + self.now + ' had   INSPECT  REPORT ' + 
                        '<br><br><font color="#1500F0"><font face="times new roman">' + 
                        'Please check the PPT_REPORT.' + '<br><br>' +
                        '</font></font><br><br>' + html_PPT_report_link +
                        mail_picture_string + 
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
                '<br><br><font face="times new roman"> NO LUM DATA at ' + self.now + '</font><br><br>'
                ''
            }
            response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send report mail successful or not: ' + str(response_01))


class alarmAutoMail(auto_mail):
    def send(self, filePath, ModelNo):
        ManualSend_01 = {
                'strMailCode': 'OOt4AZp0pAo=',
                'strRecipients':'' + self.Rmail_user + '', 
                'strCopyRecipients':'' + self.Cmail_user + '', 
                'strSubject': 'MODEL_NO Warning at ' + self.now ,
                'strBody':
                '<b><font color="#E00000">' + 
                'Hi All: ' + '</font></b>' + 
                '<br><br><font face="times new roman">' + 
                '[Warning]' + '<br><br>' + 
                'File: ' + '<font color="#1500F0">' + filePath + '</font></font><br><br>' +
                '<font face="times new roman">' + 
                'The MODEL_NO' + '<font color="#1500F0">' + ModelNo + ' was not in PRODUCT.json or cannot be recognized' + 
                '</font></font>' +
                '<font face="times new roman">' + '---- ' + self.now + '</font><br><br>'
                ''
            }
        response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send alarm mail successful or not: ' + str(response_01))

class customMessageAutoMail(auto_mail):
    def __init__(self):
        self.Rmail_user = 'Alan.YF.Liu@auo.com'
        self.Cmail_user = 'Alan.YF.Liu@auo.com'
        
    def send(self, message: str):
        ManualSend_01 = {
                'strMailCode': 'OOt4AZp0pAo=',
                'strRecipients':'' + self.Rmail_user + '', 
                'strCopyRecipients':'' + self.Cmail_user + '', 
                'strSubject': 'Not Found Warning at ' + self.now , 
                'strBody':
                '<b><font color="#E00000">' + 
                'Hi All: ' + '</font></b>' +
                '<br><br><font face="times new roman">' + message + '---- ' + self.now + '</font><br><br>'
                ''
            }
        response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send custom message mail successful or not: ' + str(response_01))


if __name__ == '__main__':

    auto_mail().sendReport('./report_production/Weekly_report.pptx', messageForTableType='Weekly')
