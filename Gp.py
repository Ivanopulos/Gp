import pandas as pd #pip install pandas matplotlib
import os  # snl_session, pdopenfail
import dill  # snl_session
import matplotlib
from tkinter import filedialog  # pdopenfail
import numpy as np
import openpyxl


def snl_session(fn="savedsession.pkl"):
    if os.path.exists(fn):  # os.path.isfile() if needed to check this folder or file
        dill.load_session(fn)
        print('session loaded')
    else:
        dill.dump_session(fn)
        print('session saved')


def pdopenfail(path=""):
    # df = pd.read_csv('apple.csv', index_col='Date', parse_dates=True)
    if path == "":
        path = filedialog.askopenfilename()  # open in explorer
    if path.find("//") > 0:  # if it full path
        print("found full path")
        print(path)
    else:
        print("guess path")  # if it not full path maybe it here or desktop
        print(os.getcwd())  # https://ru.stackoverflow.com/questions/535318/%D0%A2%D0%B5%D0%BA%D1%83%D1%89%D0%B0%D1%8F-%D0%B4%D0%B8%D1%80%D0%B5%D0%BA%D1%82%D0%BE%D1%80%D0%B8%D1%8F-%D0%B2-python
        if os.path.exists(os.getcwd() + "/" + path):
            print("found here")
            path = os.getcwd() + "/" + path
        if os.path.exists(os.environ['USERPROFILE'] + '\Desktop/' + path):
            print(os.environ['USERPROFILE'] + '\Desktop/' + path)
            path = os.environ['USERPROFILE'] + '\Desktop/' + path
    if path[len(path)-3:] == "csv":
        return pd.read_csv(path, sep=';', parse_dates={'timestamp': ['<DATE>', '<TIME>']}, index_col='timestamp')
    if path[len(path) - 4:] == "xlsx" or path[len(path) - 3:] == "xls" or path[len(path) - 4:] == "xlsm":
        return pd.read_excel(path)


# df = pdopenfail('g21.csv')
snl_session()
df.index = pd.to_datetime(df.index, format="%Y%m%d %H%M%S")
df['mean1'] = (df['<H>'] + df['<L>'])/2
df['stay1'] = (df['<H>'] - df['<L>']) == 0
df = df.loc[df['stay1'] == False]
df['time2']=df.index.strftime('%H%M').astype(float)
df = df.loc[df['time2']>1001]
df = df.drop(['<O>', '<H>', '<L>', '<C>', 'stay1', 'time2'], 1)
df1 = pd.DataFrame()  # for open file
df2 = pd.DataFrame()  # for groupby
df3 = pd.DataFrame()  # for saving concat
df4 = pd.DataFrame()
# df1['C'] = np.arange(len(df1))  # расстановка порядковых номеров
for i in range(5, 121, 5):
    df1 = df.resample("1min", label='right', closed='right').last()  # перегруппировка чтобы не ловить прыжок а крайнее смещение просто давало ошибку
    print(i)
    df1['two1'] = df1.mean1.shift(i*2) - df1.mean1.shift(i) < 0  # гност шаг(в дальнейшем должен множится)
    df1['twofin'] = df1.mean1.shift(i) - df.mean1 < 0  # фин шаг
    df1['tfm'] = df1.mean1.shift(i) - df.mean1  # эффект финального шага
    df1['na'] = df1.mean1.shift(i*2)  # метка для удаления битых данных
    df1['na1'] = df1.mean1.shift(i)  # метка для удаления битых данных(ловит только перерывы)
    df1['weday'] = df1.index.dayofweek
    df1 = df1.dropna()  # [df1.mean1 != np.NaN]
    df1['time1'] = df1.index.strftime('%H%M')
    df1['time1'] = df1['time1']  # + '-' + str(i)  # как вариант здесь делать основу для последующей группировки
    df1['t2'] = i
    df1 = df1.drop(['na', 'na1', 'mean1'], axis=1)  # сносим все строки где хоть одна ошибка
    df2 = df1.groupby(['time1', 't2', 'twofin', 'two1', 'weday']).agg({'tfm': ["sum", "size"], '<V>': ['sum']}).reset_index()  # 'tfm':["sum", "size"], '<V>':['sum']
    # нужно ли туфин в самой группировке для финальных подсчетов? где тогда хранить
    # нужен ли "модуль" в эффекте финального шага? походу нет ибо в агрегации будет давать неправильный эффект изза однонаправленности знака
    print(df2)
    df3 = pd.concat([df3, df2], axis=0)
print(df3[1:50], df3[-50:])
df3['fsst'] = df3.tfm['sum']  # finsum/size/time
df3.to_excel(r'File_Name.xlsx')
# df1.plot(kind='scatter', x='<V>', y='two')
# matplotlib.pyplot.show()
