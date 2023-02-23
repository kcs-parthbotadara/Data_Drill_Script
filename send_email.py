import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def sent_mail(email_from, email_password, email_to, batch_size, start_time, end_time, table_name, batch_no, file_name, date, inserted_rows, status):
    subject = f'Data Drill-Batch Processing {datetime.now(timezone.utc)}'
    body = f'''\
<html>
<body>
<p>Batch {batch_no} with {batch_size} data has inserted to MySQL at time {start_time} PM </p><br/><br/>
<table style="width:100%; border:1px solid black; border-collapse: collapse;">
<tr>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">START TIME</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">END TIME</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">TABLE NAME</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">BATCH NO.</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">INSERTED ROWS</th>
</tr>
<tr>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{start_time}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{end_time}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{table_name}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{batch_no}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{inserted_rows}</td>
</tr>
</table><br/><br/>

<table style="width:100%; border-collapse: collapse; border:1px solid black;">
<tr>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">BATCH SIZE</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">DATE</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">FILE NAME</th>
    <th style="border:1px solid black; border-collapse: collapse; background-color: #ff8b3d;">STATUS</th>
</tr>
<tr>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{batch_size}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{date}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{file_name}</td>
    <td style="border:1px solid black; border-collapse: collapse; text-align: center;">{status}</td>
</tr>
</table><br/><br/>
</body>
</html>
'''

    email_message = MIMEMultipart()
    email_message['From'] = email_from
    email_message['To'] = ", ".join(email_to)
    email_message['Subject'] = subject
    email_message.attach(MIMEText(body, 'html'))

    email_string = email_message.as_string()

    try:
        smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        smtp_server.ehlo()
        smtp_server.login(email_from, email_password)
        smtp_server.sendmail(email_from, email_to, email_string)
        smtp_server.close()
        return "Email sent successfully!"
    except Exception as ex:
        return f"Something went wrong…. {ex}"
