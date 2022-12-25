"""
This code snippet will use pandas module to process data of dhcp.log file
"""
import re
import pandas as pd
from data_json import data_json

lines=[]


#data_valuelist = list(data_json.values())
def data_process():
# open the log file
    with open("/home/saravana/Desktop/dhcp.log") as fi:
        for line in fi:
            line = re.split(r"\s+", line, 12) # split only 12 times per line
            lines.append(line)         # build a list of list

    df=pd.DataFrame(lines) # create dataframe
    df.loc[df[12] == "duplicate on 192.168.12.0/24\n", 12] = "duplicate" # Convert duplicate on 192.168.12.0/24\n to duplicate in column value of column 12
    df3 = df.drop_duplicates(subset=[9], keep='first') # Dropping all duplicate column values of MAC Address
    df.to_csv('nodes.csv', index=None) # Converting dataframe to csv file
    df3["Manufacturers"] = df3[9].map(data_json) # Mapping values of data_json dictionary to datafram column values and accordingly creating a new column Manufacturers
    df3 = df3.dropna(subset=['Manufacturers']) # Dropping None values in Manufacturers column
    df3.columns = ['Month',0 , 'Day', 'Time', 'Protocol', 'Protocol_id', 6, 'IP', 8, 'MAC Address', 'Host Name', 11, 12, 'Manufacturers'] # Renaming column names
    df3 = df3.drop([0, 6, 8, 11, 12, "Day", "Time", "Month", "Protocol", "Protocol_id"], axis=1) # Dropping unnecessary columns
    df3.loc[df3["Host Name"] == "via", "Host Name"] = None # Replacing Hostname value via to None
    df3.to_csv('nodes_modified.csv', index=None) # Converting final output dataframe to csv file
    #print(df)
    print(df3)

data_process()