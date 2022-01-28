import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import re
import pandas as pd

sheet_keys = [
    'https://docs.google.com/spreadsheets/d/1NkqiisJONM45d5qTpBrS6xK4-1GZdqPHJwQ2NHiVhns/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1wDqetuhC3vjd5u_b3hl_sBKI3ZYP6lgcjRUQTgnzr_A/edit?usp=sharing'
    ]

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("TPESE_KEY.json", scopes)

others_route = "./others.csv"
with open(others_route, "w", encoding="utf-8") as file:
    file.write("system_id,sent_date__c,content_name__c,journey_content__r_a_b_test__c,type__c,card_no__c,card_type__c,status__c,arrival_station__c,departure_station__c,pnr_number,utm_content__c,birthday_event_date\n")

open("./which_row.txt", "w").close()

sheet_key_counts = 0
sheet_row_counts = 1
for i in sheet_keys:


    sheet_key_counts += 1

    time.sleep(3)
    client = gspread.authorize(credentials)
    the_key = (re.match('.*d\/(.*)\/edit.*', i)[1])
    sheets = client.open_by_key(the_key).sheet1
    file_name = client.open_by_key(the_key).title

    values = sheets.get("A:M")
    rows = sheets.get("A:A")

    with open("./history/{file_name}.csv" .format(file_name=file_name), "w", encoding="utf-8") as file:
        file.write("system_id,sent_date__c,content_name__c,journey_content__r_a_b_test__c,type__c,card_no__c,card_type__c,status__c,arrival_station__c,departure_station__c,pnr_number,utm_content__c,birthday_event_date\n")
        for j in range(1, len(values)):
            for k in range(0, 13):
                if k==12:
                    try:
                        if(values[j][k]=="0"):
                            file.write("\n")
                        elif(values[j][k]=="1"):
                            file.write("\n")
                        else:
                            file.write(values[j][k]+"\n")
                    except:
                        file.write("\n")
                        continue
                elif k<12:
                    try:
                        if(values[j][k]=="0"):
                            file.write(",")
                        elif(values[j][k]=="1"):
                            file.write(",")
                        else:
                            file.write(values[j][k]+",")
                    except:
                        file.write(",")
                        continue
    
    with open(others_route, "a", encoding="utf-8") as file:
        for j in range(1, len(values)):
            for k in range(0, len(values[j])):
                if values[j][k]=="1":
                    file.write("\n")
                    break
                elif values[j][k]=="0":
                    file.write("\n")
                    break
                elif k==len(values[j])-1:
                    file.write(values[j][k]+"\n")
                else:
                    file.write(values[j][k]+",")

    row_start = sheet_row_counts + 1
    sheet_row_counts = sheet_row_counts + len(rows) - 1
    log_message = ("{sheet_key_counts}/{sheet_key_length},{file_name},{sheet_title},{row_start}-{row_end}" .format(sheet_key_counts=sheet_key_counts, sheet_key_length=len(sheet_keys), file_name=file_name, sheet_title=sheets.title, row_start=row_start, row_end=sheet_row_counts))
    with open("./which_row.csv", "a", encoding="utf-8") as which_row:
        which_row.write(log_message + "\n")
        print(log_message)
