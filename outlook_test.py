import win32com.client as client
import datetime
import pytz

outlook = client.Dispatch('Outlook.Application')
namespace = outlook.GetNameSpace('MAPI')
account = namespace.Folders['data@interlake-steamship.com']
inbox = account.Folders['Inbox']

today = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
yesterday = today - datetime.timedelta(days = 3)
ystrdy = yesterday.strftime("%d-%m-%y")

print(ystrdy)

Inbox_all = [message for message in inbox.Items]
MMCaptain_Emails = [message for message in inbox.Items if message.subject.startswith('Mesabi Miner')]
LATCaptain_Emails = [message for message in inbox.Items if message.subject.startswith('Lee A Tregurtha')]
PRTCaptain_Emails = [message for message in inbox.Items if message.subject.startswith('Paul R Tregurtha')]
HJLOCaptain_Emails = [message for message in inbox.Items if message.subject.startswith('HJLO')]
JRBCaptain_Emails = [message for message in inbox.Items if message.subject.startswith('James R Barker')]

#EGCS_MM_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed'].Folders['EGCS MM']
#EGCS_LAT_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed'].Folders['EGCS LAT']
#EGCS_PRT_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed'].Folders['EGCS PRT']
EGCS_HJLO_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed']
#EGCS_JRB_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed'].Folders['EGCS JRB']
#EGCS_body_email_search = [message for message in inbox.Items if 'URGENT' in message.Body.lower()]
#new_folder_create = inbox.Folders.Add('Overflow')
#EGCS_JRB_Processed_Folder = account.Folders['MAST'].Folders['EGCS'].Folders['Processed'].Folders['EGCS JRB']

test = [message for message in EGCS_HJLO_Processed_Folder.Items]

print(test)

# for message in MMCaptain_Emails:
#     message.Move(EGCS_MM_Processed_Folder)
#
# for message in LATCaptain_Emails:
#     message.Move(EGCS_LAT_Processed_Folder)
#
# for message in PRTCaptain_Emails:
#     message.Move(EGCS_PRT_Processed_Folder)
#
# for message in JRBCaptain_Emails:
#     message.Move(EGCS_JRB_Processed_Folder)

#print(d)

# current_message = inbox.getfirst()
# while current_message:
#     msg_date = current_message.SentOn.strftime("%d-%m-%y")
#     sjl = current_message.Subject
#     if d < msg_date:
#         print(sjl)
#     current_message = inbox.getNext()

# for message in Inbox_all:
#     msg_date_s = message.SentOn.strftime("%d-%m-%y")
#     yesterday_s = yesterday.strftime("%d-%m-%y")
#     msg_date = message.SentOn
#     sjl = message.Subject
#     if msg_date > yesterday:
#         print("Message date is " + msg_date_s + " yesterday was: " + yesterday_s + " subject is: " + sjl)
#
