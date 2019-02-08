######## Functions to manipulate text for data science #########
#
# Author: Arthur Telles
#
# Description: 
# 
# This script contains email functions capable of attaching files.
# 
# -------------------- USAGE -------------------- #
#
##### - send_mail - #
#
# Description: Simple function developed for sending small to medium amounts of 
# emails with attachments.
#
# * INPUT *
# ---------
# 1. config_dict - dictionary contanining configuration info like host, port, 
# user, password and from_addr
# 2. send_to - recipient
# 3. subject
# 4. message - full body message
#

import smtplib
import os.path as op
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders


def send_mail(config_dict,send_to, subject, message, files=[],
              use_tls=True):
    username = str(config_dict['user'])
    password = str(config_dict['password'])
    port = str(config_dict['port'])
    server = str(config_dict['host'])
    send_from = str(config_dict['from_addr'])
    """Compose and send email with provided info and attachments.

    Args:
        send_from (str): from name
        send_to (str): to name
        subject (str): message title
        message (str): message body
        files (list[str]): list of file paths to be attached to email
        server (str): mail server host name
        port (int): port number
        username (str): server auth username
        password (str): server auth password
        use_tls (bool): use TLS mode
    """
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(message))

    for path in files:
        part = MIMEBase('application', "octet-stream")
        with open(path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="{}"'.format(op.basename(path)))
        msg.attach(part)

    smtp = smtplib.SMTP_SSL(server, port)

    smtp.login(username, password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()
    print('Successfully sent the email')