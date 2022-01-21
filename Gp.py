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

def step_1_to21():
    global sdf
    global df
    sdf = []
    sdf = pd.DataFrame(sdf)
    for file in os.listdir():
        if file.endswith(".csv"):
            df = pdopenfail(file)
            if not file=="G21.csv":
                sdf = pd.concat([sdf, df], axis=0) # из расчета что г21 последний и сохранится в df
            print(file)

#step_1_to21()#df в резерве на пробный обсчет результатов
snl_session("step_1_to21")
#V1 серия поиска корреляции объема гност периода с вероятностью прогноза, итог отрицательный
sdf = sdf.rename(columns={'<VOL>': '<V>', '<OPEN>': '<O>', '<CLOSE>': '<C>', '<LOW>': '<L>', '<HIGH>': '<H>'})
df = df.rename(columns={'<VOL>': '<V>', '<OPEN>': '<O>', '<CLOSE>': '<C>', '<LOW>': '<L>', '<HIGH>': '<H>'})
sdf.index = pd.to_datetime(sdf.index, format="%Y%m%d %H%M%S")
sdf['mean1'] = (sdf['<H>'] + sdf['<L>']) / 2
sdf['stay1'] = (sdf['<H>'] - sdf['<L>']) == 0
sdf = sdf.loc[sdf['stay1'] == False]
sdf['time2'] = sdf.index.strftime('%H%M').astype(float)
sdf = sdf.loc[sdf['time2'] > 1001]  # убираем все что меньше 1001
sdf = sdf.drop(['<O>', '<H>', '<L>', '<C>', 'stay1', 'time2', '<TICKER>', '<PER>'], 1)
df1 = pd.DataFrame()  # for open file
df2 = pd.DataFrame()  # for groupby
df3 = pd.DataFrame()  # for saving concat
df4 = pd.DataFrame()
for i in range(5, 121, 5):
    df1 = sdf.resample("1min", label='right', closed='right').last()  # перегруппировка чтобы не ловить прыжок, а крайнее смещение просто давало ошибку, перестает работать на круглосуточных вариантах
    print(i)
    df1['b_gnost'] = df1.mean1.shift(i * 2) - df1.mean1.shift(i) < 0  # value гностического шага (в дальнейшем должен множится)
    df1['b_fin'] = df1.mean1.shift(i) - df1.mean1 < 0  # фин шаг
    df1['v_fin'] = df1.mean1 - df1.mean1.shift(i)  # эффект финального шага
    df1['na'] = df1.mean1.shift(i * 2)  # метка для удаления битых данных
    df1['na1'] = df1.mean1.shift(i)  # метка для удаления битых данных(ловит только перерывы)
    df1['weday'] = df1.index.dayofweek
    #V1 df1['V'] = df1['<V>'].rolling(window=i).sum().shift(i)/i  # df.rolling('2s').sum() # суммируем i минут c -i*2 до -i строки # объем предпосылки в минуту

    df1 = df1.dropna()  # [df1.mean1 != np.NaN] сносим все строки где хоть одна ошибка

    df1['t_of_end'] = df1.index.strftime('%H%M')  # время полного окончания хода
    df1['t_step'] = i  # время=размер шага
    df1 = df1.drop(['na', 'na1', 'mean1'], axis=1)  #
    #V1 df2 = df1.groupby(['t_of_end', 't_step', 'b_gnost', 'b_fin', 'weday']).agg({'v_fin': ["sum", "size"], 'V': ['mean']}).reset_index()  # 'v_fin':["sum", "size"], '<V>':['sum']  '<V>': ['mean'] как вариант учета напряжения
    df2 = df1.groupby(['t_of_end', 't_step', 'b_gnost', 'weday']).agg(
        {'v_fin': ["sum", "size"]}).reset_index()
    df2['v_fin_abs'] = abs(df2.v_fin['sum'])  # модуль эффекта хода
    df2[('v_fin', 'sum')] = df2[('v_fin', 'sum')]>0  # знак хода
    df2 = df2.rename(columns={('v_fin', 'sum'): ('v_fin', 'bfin')})
    print(df2)
    # нужно ли туфин в самой группировке для финальных подсчетов? где тогда хранить
    df3 = pd.concat([df3, df2], axis=0)
print(df3.shape, df3.columns, df3.dtypes, df3.describe())
#df3['fsst'] = df3.v_fin['sum'] / df3.v_fin['sum']  # средний вес на веру в событие
print(df3[1:50], df3[-70:])
df3.to_excel(r'File_Name.xlsx')



# df1.plot(kind='scatter', x='<V>', y='two')
# matplotlib.pyplot.show()

#df.loc[(df['a'] == 1) & (df['c'] == 2), 'b'].sum() С помощью этого метода вы выясняете, где столбец 'a' равен 1 , а затем суммируете соответствующие строки столбца 'b'. Вы можете использовать loc для обработки индексации строк и столбцов:
#df.query("a == 1 and c == 2")['b'].sum()
#df.groupby('a')['b'].sum()[1] Альтернативный подход заключается в использовании groupby для разделения DataFrame на части в соответствии со значением в столбце 'a'. Затем вы можете суммировать каждую часть и вытащить значение, к которому сложились 1
#df[df['a']==1]['b'].sum()      sum(df[df['a']==1]['b'])   np.where(df['a']==1, df['b'],0).sum()
