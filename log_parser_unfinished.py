import pandas as pd
import datetime as dt

# Takes the path to the file as input. Returns a table with data from the log. Pandas, datetime requred
def log_to_df (file):
    # list of months, used in postfix log file
    month_names_d = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    with open (file, 'r') as mlog:
        log_dict_gmgr = {'index':[], 'time':[], 'sender':[], 'msize':[]}
        log_dict_smtp = {'index':[], 'recipient':[], 'ifhel':[], 'status':[]}
        for line in mlog:
            line = line.strip().split()
            if len(line) > 7 and line[7][:5] == 'size=':
                log_dict_gmgr['index'].append(line[5][:-1])
                log_dict_gmgr['time'].append(dt.datetime(dt.datetime.now().year, month_names_d[line[0]], int(line[1]), int(line[2][:2]), int(line[2][3:5]), int(line[2][6:])))
                log_dict_gmgr['sender'].append(line[6][6:-2])
                log_dict_gmgr['msize'].append(int(line[7][5:-1]))
            elif len(line) > 11 and line[11][:6] == 'status':
                log_dict_smtp['recipient'].append(line[6][4:-2])
                addr = line[6][4:-2].split('@')[1]
                if addr == 'spb.helix.ru' or addr == 'helix.ru':
                    log_dict_smtp['ifhel'].append(1)
                else:
                    log_dict_smtp['ifhel'].append(0)
                log_dict_smtp['status'].append(line[11][7:])
                log_dict_smtp['index'].append(line[5][:-1])
        df_gmgr, df_smtp = pd.DataFrame(log_dict_gmgr), pd.DataFrame(log_dict_smtp)
        return (df_gmgr.merge(df_smtp, left_on='index', right_on='index'))

file = 'mail.log'  

log_to_df(file).to_csv('log_data_frame.csv')