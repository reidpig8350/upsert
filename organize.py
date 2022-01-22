import re
import pandas as pd

others_route = "./others.csv"
with open(others_route, "w", encoding="utf-8") as file:
    file.write("system_id,sent_date__c,content_name__c,journey_content__r_a_b_test__c,type__c,card_no__c,card_type__c,status__c,arrival_station__c,departure_station__c,pnr_number,utm_content__c,birthday_event_date\n")

open("./which_row1.txt", "w").close()

sheet_key_counts = 0
sheet_row_counts = 1
for day_order in range(6,31):
    sheet_key_counts += 1
    month_order = 10

    file_name_re = re.match("./history/.*{month}月{day}日.*\.csv" .format(month=month_order, day=day_order))
    with open(file_name_re, 'r',encoding="utf-8") as journey:
        with open(others_route, 'a', encoding='utf-8') as log_file:
            write_file = pd.read_csv(journey)
            log_file.write(write_file)