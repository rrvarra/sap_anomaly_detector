# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 06:50:42 2018

@author: ad_sarkardi
"""
import logging

import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart


def send_html_email_alert(receiver_list, sender, title, message):


    try:
        # create smtp mail server object
        smtp_server = smtplib.SMTP('smtp.intel.com')

        # construct email
        msg = MIMEMultipart('alternative')
        msg['From'] = sender
        msg['To'] = ', '.join(receiver_list)
        msg['Subject'] = title
        msg.add_header('Content-Type','text/html')
        
        # get email content
        msg.attach(MIMEText(message, 'html'))   

        # send email
        smtp_server.sendmail(msg['From'], receiver_list, msg.as_string())
        smtp_server.quit()

    except Exception as ex:
        logging.error('FAILED To send email: %s', str(ex))        
        return "FAILURE"
    return "SUCCESS"