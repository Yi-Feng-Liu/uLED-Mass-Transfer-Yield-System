import zeep
from datetime import datetime


class auto_mail():
    def __init__(self):
        self.Rmail_user = "Alan.YF.Liu@mail.com"
        self.Cmail_user = "Alan.YF.Liu@mail.com"
        self.now = datetime.now().strftime("%Y%m%d%H%M")
        self.client = zeep.Client("http://ids.cdn.corpnet.mail.com/IDS_WS/Mail.asmx?wsdl") 
        
        # 連到放圖片的 server
        # ssh = paramiko.SSHClient()
        # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh.connect("ltwma01", 22, "wma", "wma")
        # self.sftp_client = ssh.open_sftp()


class customMessageAutoMail(auto_mail):
    """Send the specific subject and message to people

    Args:
        subject (str)
        message (str)
    """
    
    def __init__(self):
        super(customMessageAutoMail, self).__init__()
        self.Rmail_user = "Alan.YF.Liu@mail.com"
        self.Cmail_user = "Alan.YF.Liu@mail.com"
    
    
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
                '<br><br><font face="Noto Sans TC">' + message + '---- ' + self.now + '</font><br><br>'
                ''
            }
        response_01 = self.client.service.ManualSend_07(**ManualSend_01)
        print('Send custom message mail successful or not: ' + str(response_01))

