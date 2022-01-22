import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import re
import pandas as pd

sheet_keys = ['https://docs.google.com/spreadsheets/d/1-1bdmXZXYwOTa4TMgM6Uqnad0eFfDyqtf4GGEfkrrXQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/10LM1VZfHhemd_6vNfxIcSEX7AIElnZAG-XVtsbeHkLQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/10i-a3GGqbUnNILFh6Tge88weIwH1VtLnwXQYWR1cD6A/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/12XSjrvtAOp-AqpUAjsXm4ixMmbXzH0-pN842H_S_6H4/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/13L6CHdw6yLaBQoZdwLd-rdvEHbIcwnFupJLOCi70DRc/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/13LdQp8ibU-w_XrS_UsOj3qsFUO00LYDxBS62bSF3fYg/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/13mcgZZ9TyeGThHKecBPb1lo79Yt9VSQ56WH8k98Ir2w/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/14kwKv2rMyFKxZIv0HBGTJsgm5xSS9yIbfijmQgITAQE/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/14oz-icEASZb0YbKSogOYSHipEwD-4j-PPRDq0pbBMxU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/15foj7arkk9ZeMGPmN3Dtg9MgvpLpr3ZWZA1etuaMQl0/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/15r5RWh3knyB8C7Yv66OcnajFxGVwaOlNicojKmxKUgo/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/17-Fm-N_gx1CM1F4CnH12f4oy9K8koeP0_V18mGXlh1c/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/18PCiaqlCNvITIlC4w7Y-DRlQAQWSZoJINeXL4FUeQd0/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/19v6Up8Clxqne748wZ_egAQgORzsdJx_2-xWNyXsG2N4/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1Ac21mXp62olFRg7pfU9EJuh8FY0404O3yhe76NOrYcg/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1BeRc-TFYx8GfugfOSNYL_mm_IXTqE9X3iO_yxS2zY7I/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1BiwhIJA7DhT8OP8kIeBd8YcEHi8Dgf8S8Vu_7WQzLbE/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1CY1PnA6iUVnR7mzH0QSdvUa1DS9tVH9VmeQR2RQpKFM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1CkIkw3m7_o-dI5TDuq9ui6jRV-q2xAHdILxvytYqaSY/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1DnQqeNl9KQWR7daQr0Mz-TKC_EBwmJJLYLOfbyA3q0A/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1DsTvwx1Ds5q53LC9jHIlcbEPg20XUoq5-0JuWWCRtwY/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1EqTYyjIH6bMWgDeGLvXylGFqn-z9ILPo9ukWJ1Dbla8/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1EqnxoLYrsUzL0JwDvJUEf58E75z_PUJznHwn6KxdVaU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1FBiqzinWAsJMrHFrkmUduOgUV_ZXBtj0DlI98p9FXAs/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1FU4EY3L2AGEkUK-pNcPeSlWyGqsfzTbwnMIUZpmSgUM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1FqHH9MDgeIq6D6VTEOonTd7kOvPoJO0ayOVxayDymXk/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1GCnGb-e2p-tpzlN8guYC7UugTGUCEZpH5wtaWqNtQGw/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1GswzmS8LriSpvTTw3DdJO3u20LPh4nY7DGqWjf77EwA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1Hz575VnDWQnLwlDD9ee94UlGukBcH08slVNNloHUTUs/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1IgGLVlyOO0xWNj6LttXsqxUXx0ACY4Nh7ITVKzc_DN0/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1JahkIOBhzNgfj1-I_P34E7d6ZS4ivrNCaw4iWxkqIpY/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1JslIXmNW8n6ZCZfVDcM9tn1oHU--h2PhBHPsFPmurto/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1LPBymSqK6QUq-wnZmC2rLHjmGxS-LT0D9unp2Cl553s/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1MnleF_DF4-1xYTNRpP-0g7HsYSFH1rBcpqI7fojZ8z4/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1O7HXx3T91y-Cw1t0neTleP6jop0l5e7km3QW3HCp8LM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1PPnBe3tV9M--hmrM-UpYTNr5aV-R1sZHGIle7K8BIVM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1PzN8LvEw3L_r8iG7r1aTxsi22Gb3p6sAulsK9in0XVw/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1R0EbLLz3suLNvxCTSOWjZ-noENMgYCUdB7Qd4ZJn4iQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1R19Gd3KeVs6yTKn7oyKvJjIEt3fxpRkeWaIQ_--WZwg/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1R7NP8hhAJYta52skLM8lgJ2Ymi93Unq5AQkZjmBRQyc/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1SVQi-SnpnnQTbq3Gh-ZmCsFjqEX2CND7xIjPBguWvtE/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1SZdPDJPf_eQ7pc3S72SUvaB7N5wWa3Z_AxgXFY7duX4/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1TL9ExKGxnqRMY5SQuF7-crxxohcDNL2BI9TNcX5i1vA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1TYOnCIQ1u7JDUR--Xni1sf0IN6zyLGQnjr1_U4YgDIg/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1TYxGh2EKHTasgdO-QvnKUXYNT0vDRJ8L3Z1wLq1-MnM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1TdaxX0M95S5Uf2F6x7JTsuHrlRnpdIqj8JXv0ZG2MyM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1U9tWOTkJ-rsHKkgJ9iJ9j6hrZX3KZnBRR7GOUKtpBdY/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1UptPEWVURzNXoKhf6iqF48V7jG9V66Kui-21k5rF9TA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1Uwphk_eOPDEEXjTJwgTjlWmBQLZs3l9DhlQfeh8-7uw/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1VNnodVxrlGU9vSCxCFMy2sSvWBIdz4hjPNYn5p4spXA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1WGeg-5GLEOz46sm9M0IfGl6FI0zRIbBc9TP_zYzhlkQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1Xa6ZU2nndRiFKsWiV7iOniet5vVqMQJLbyZXNJ6Ggdk/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1Xeoe3O8VhYJsPM6NoB4atU6t3Y6kemPVtMLvKnPrtdU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1ZVyYb1ctchBBmSk-AV2m5UwjboWfeT8nr2gjEKSPuzg/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1_kOXs-5plRTVtMMV2FPoiYjNCHxy87Ey53lVPNyGtTY/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1a99OBs8rjhOeqH9AzZrVxA9fFakvLUQ10NedRjbngXA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1aeBAvfJb_L2WnuZxQw9Yiw10vudUad5ganRGI1EnncI/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1bX16328P-GQfeRnl21J4kf9-D79GXwevrrX3ULIs-ok/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1c9m8AescvmFRsAQsSE86qEKZnQlR_0VXMo7cNuoPbsE/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1cfYX30VhePKH0FcIdVuKtT-U8M_AqT1JMHIjon-aw9I/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1dPG4pmqmnmAg6VBKeudj7g48U9zFCC2faFLaLGxh2qQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1f2NWyIv2BPKXJ3kVvtptpVpeNRHvIwbrjFHDjbuzRK8/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1fff7kLvS7X8RJuUk7w1WSz1XISetJx9zuPIPbVYhcqM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1fsi0UQcTO51uR1Uy03wUwbehy2HDXMIF0xECu1EWJf4/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1gSs-5V21BwCA3PH_h5dP9nvSp4nZZqcZQUWkViZ2elI/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1hLC24JDZ0exA6T9e7pshCB7LgVmFAolvJnvckl6IkU8/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1hLOAfoL9oMFl1WytGHpFI8FMw-r0luQv5jSVFjHhVoU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1hT1e2Qk5P4MsC34kJJiqaPasNx-8CPk6jIrTJKH0uZI/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1ivJE3dxYeZGAJ7I1_ojIldm2ZyoFiB6osMLB5ybDjo8/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1j_lcPsRFVfH8x99jjUFkZGehbtX2zRPTPgTKRNEro-c/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1jtWNIKzZdWQd0EEYQnjT8OODZh2ijvUvazi-6T41S2M/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1kr7xO3kR95jQVac1O5iCijzvkz-2dhqucPTprd4KpzM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1ktCkmTm5agULfBC0kCKoxqMy5UZ1Ad9ifnB0sCe9C0w/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1lxQVns9zWkb7EVMaxIb0s_gWWsNYSIk5pRe9TqGBCuU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1m9r9z9gEoXx5tce95IlqtvPq2vQPzNogAVgeQDUpfZ8/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1nWyikeFzZL-ex5Q4WMPGUmZA2qr9JrkWEFQNkhJPhqM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1nXMavTQNlu7w3oNzVLxiSCr2iVY2aw_UbwVIdnYsnqk/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1ntX98Bn1ybkXo2HlrXrLBRp4aRHyQs9lzd_yNJo3xrU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1oF4oYlrs5f7hNYhQKLzn41EyXFOSOe54xupHtF9hybA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1pdKhfIwYzd0Q5vUv3ioACZ9IRm_SGkarfrLFZ1wVvhA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1qJYVhxInPNpPFvgf31ApZ-DZDPRt__h3nOEXo1s9zpQ/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1qKj6X1vK1xsr3wcEKkWCXF_WOkQLPuErC-X4XnZ4_4o/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1qQQZEMisfjTI5srltmaAYaBz-EYm9F2P8WLRM9m8smU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1rEBM4P_ppx1Hp1M68xIAqbXt0XROe0q1swM-qikZTqs/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1racu4JINOOEGg5JCWGrD-vD14Y-GjYnShN5L1EE3Rwc/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1uPOAAyG0sB3q5rhXbZmSN2o7MeOyjrkJr8SKo-KKMrM/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1utxt04a50Vzh5zdjXoPDfMftO7WDUZ7o4dcXc-imi5Y/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1uufHAT8niYRkRfguwljgE2DivSvCIqLyd0N6KRhRE9Q/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1v8qDP2Iu7nUSct9zaizhwRkFRNCg_9pnBDi1gkSIdcU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1vliIUJ34cog4QKvpeVuoXMXVKwIWR83TH8Uco5vjI_Q/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1vq_DYdZS_Po0gH02klow_QyYNzENVtLL26cbmllQLgA/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1w1pjcvznCd_pabuDwRVjUWL9rKCx902DhQ1Kla-YUOc/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1wMGPIrloAFAdvanoB_1eDoqBFdKzUgXmLNqvJ-4SEBU/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1wTYf27pThf3x5wFXzJxSLtT0PAcWPuGlH5jnQ6x6BPw/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1w_sDjuPu9Vc8MAakQjY1Vg4CZ4tf4jmmA7X4BQJXs04/edit?usp=sharing', 'https://docs.google.com/spreadsheets/d/1xlINuVCCGKFThqq_YTQk998V1AjMlMgz0Uy4kPVUfiA/edit?usp=sharing']
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
    log_message = ("{sheet_key_counts}/{sheet_key_length} {file_name} {sheet_title} {row_start}-{row_end}" .format(sheet_key_counts=sheet_key_counts, sheet_key_length=len(sheet_keys), file_name=file_name, sheet_title=sheets.title, row_start=row_start, row_end=sheet_row_counts))
    with open("./which_row.txt", "a", encoding="utf-8") as which_row:
        which_row.write(log_message + "\n")
        print(log_message)
