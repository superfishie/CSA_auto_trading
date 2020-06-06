# -*- coding:utf-8 -*-

"""

@author:    zmate 
            Jiang Ningwang
            RaoYibo

@time:      18-11-10 下午4:21

"""

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
# from email.mime.image import MIMEImage
from util.tool_datetime import ToolDateTime
from email.mime.base import MIMEBase
from email import encoders


class ToolSendMail(object):
    @staticmethod
    def _add_text_to_message(message_text, message):
        """
        # 往message中添加普通文本
        :param message_text:
        :param message:
        :return:
        """
        if message_text:
            df_str = """
            <br>
            <b>%s </b>
            <br>
            """ % message_text
            message = ToolSendMail.add_before(df_str, message, before="</body>")
        return message

    @staticmethod
    def add_before(df_str, message, before):
        index = message.index(before)
        while 1:
            if message[index] != "\n":
                index -= 1
            else:
                break
        message = message[:index] + df_str + message[index:]
        return message

    @staticmethod
    def _add_img_to_message(img_path_list, message):
        """
        往message中添加img的html位置
        :param img_path_list: 格式示例：[{"img_title":图片名称,"img_path":image路径}，{"img_title":图片名称,"img_path":image路径}]
        :param message:
        :return:
        """
        if img_path_list:
            for i in range(len(img_path_list)):
                img_title = img_path_list[i].get("img_title")
                img_path = img_path_list[i].get("img_path")
                df_str = """
                <br>
                <h3>%s：</h3>
                <br>
                <p><img src="cid:image%s"></p>
                <br>
                 """ % (img_title, i)
                message = ToolSendMail.add_before(df_str, message, before="</body>")
        return message

    @staticmethod
    def _attach_img_to_msg(img_path_list, msg):
        """
        添加图片附件到msg
        :param img_path_list: 格式示例：[{"img_title":图片名称,"img_path":image路径}，{"img_title":图片名称,"img_path":image路径}]
        :param msg:
        :return:
        """
        if img_path_list:
            for i in range(len(img_path_list)):
                img_title = img_path_list[i].get("img_title")
                img_path = img_path_list[i].get("img_path")

                # 指定图片为当前目录
                fp = open(img_path, 'rb')
                msg_image = MIMEImage(fp.read())
                fp.close()

                # 定义图片 ID，在 HTML 文本中引用
                msg_image.add_header('Content-ID', '<image%s>' % i)
                msg.attach(msg_image)

    @staticmethod
    def _add_df_to_message(df_obj_list, message):
        """
        往message中添加df的html位置
        :param df_obj_list: 格式示例：[{"df_title":df名称,"df_obj":df对象}，{"df_title":df名称,"df_obj":df对象}]
        :param message:
        :return:
        """
        if df_obj_list:
            for i in range(len(df_obj_list)):
                df_title = df_obj_list[i].get("df_title")
                df_obj = df_obj_list[i].get("df_obj")
                df_str = """
                <br>
                <b>%s： %s</b>
                <br>
                """ % (df_title, df_obj.to_html())
                message = ToolSendMail.add_before(df_str, message, before="</body>")
        return message

    @staticmethod
    def _init_smtp_obj():
        """
        创建 SMTP 对象
        :return: smtp
        """
        smtp_host = 'smtp.exmail.qq.com'  # SMTP 服务器主机
        smtp_port = 465  # SMTP 服务器端口号
        smtp_obj = smtplib.SMTP_SSL(host=smtp_host, port=smtp_port)
        return smtp_obj

    @staticmethod
    def send_attach_mail(mail_title="邮件标题", message_title="邮件正文标题", message_text=None, df_obj_list=None, img_path_list=None, from_addr=None, password=None, to_addrs=None):
        """

        :param mail_title: 邮件的标题
        :param message_title: 邮件正文的标题
        :param message_text: 邮件正文的文字
        :param df_obj_list: 格式示例：[{"df_title":df名称,"df_obj":df对象}，{"df_title":df名称,"df_obj":df对象}]
        :param img_path_list: 格式示例：[{"img_title":图片名称,"img_path":image路径}，{"img_title":图片名称,"img_path":image路径}]
        :param from_addr: 账号
        :param password: 密码
        :param to_addrs: 目标邮箱地址['邮件地址一'，'邮件地址二']
        :return:
        """
        print("send_attach_mail")
        # 创建 SMTP 对象
        smtp_obj = ToolSendMail._init_smtp_obj()

        # 发送者、接收者
        if not from_addr:
            from_addr = 'dev@zmate.cn'
        if not password:
            password = 'Dev2016@zmate'
        if to_addrs:
            if isinstance(to_addrs, list):
                pass
            if isinstance(to_addrs, str):
                to_addrs = [to_addrs]
        else:
            to_addrs = ['liuqiang@zmate.cn']

        msg = MIMEMultipart()
        msg["Subject"] = Header(s=mail_title, charset="utf-8")  # 标题
        msg["From"] = Header(s=from_addr)  # 发送者
        msg["To"] = Header(s='; '.join(to_addrs))  # 接收者

        # 邮件正文默认格式
        message = """
                <html>
                <head>
                    <meta charset="UTF-8">
                </head>
                <body>
                <h1 align="center">%s</h1>
                <b>本邮件发送时间： %s</b>
                <br>
                </body>
                </html>
                """ % (message_title, ToolDateTime.get_date_string("s"))

        # 往message中添加普通文本
        message = ToolSendMail._add_text_to_message(message_text, message)
        # 往message中添加df的html
        message = ToolSendMail._add_df_to_message(df_obj_list, message)
        # 往message中添加图片的html
        message = ToolSendMail._add_img_to_message(img_path_list, message)

        # 添加message到msg
        msg.attach(payload=MIMEText(_text=message, _subtype="html", _charset="utf-8"))

        # 添加图片附件到msg
        # ToolSendMail._attach_img_to_msg(img_path_list, msg)

        # 使用 SMTP 对象发送邮件
        smtp_obj.login(user=from_addr, password=password)
        smtp_obj.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=msg.as_string())
        smtp_obj.quit()


if __name__ == '__main__':
        ToolSendMail.send_attach_mail(to_addrs=['liuqiang@zmate.cn'], message_title='lq', message_text='haha')