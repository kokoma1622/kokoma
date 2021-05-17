import pandas as pd
import numpy as np
import random
import urllib
import pyodbc
from sqlalchemy import create_engine


server = "kokoma.database.windows.net"
database = "v11_01_"
username = "kokoma"
password = "qkr741*963"
driver = '{ODBC Driver 17 for SQL Server}'

db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
#db_list = ['mid', 'bot', 'sup']
#db_list = ['sup']
engine = {}

for db in db_list:
  odbc_str = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;UID='+username+';DATABASE='+ database + db + ';PWD='+ password
  connect_str = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbc_str)
  engine[db] = create_engine(connect_str, fast_executemany=True, echo=False)



table_list = ['result', 'detail', 'laning']
column_list = {'result': ['contribution', 'death_ratio', 'gold_pm', 'cs_pm'],
               'detail': ['dmg_dealt_champ_pm', 'dmg_dealt_object_pm' ,'dmg_dealt_tower_pm', 'dmg_taken_pm', 'dmg_mitigated_pm', 'total_heal_pm', 'cs_minion_pm', 'cs_jungle_pm',
                          'cs_jungle_team_pm', 'cs_jungle_enemy_pm', 'vision_score_pm', 'ward_bought_pm', 'ward_placed_pm', 'ward_killed_pm'],
               'laning': ['level_14min', 'xp_14min', 'gold_14min', 'cs_14min', 'kills_14min', 'deaths_14min', 'assists_14min']}

key, data_front = {}, {}
data_self_normal, data_diff_normal = {}, {}
data_self_zscore, data_diff_zscore = {}, {}


for db in db_list:
  data_self_normal[db], data_diff_normal[db] = {}, {}
  data_self_zscore[db], data_diff_zscore[db] = {}, {}

  for table in table_list:
    stat = pd.DataFrame(columns=column_list[table])

    for i in range(10):
      data = pd.read_sql('select * from interval%d_data_%s_self order by keyy' % (i, table), con=engine[db]).to_numpy()

      if i == 0:
        data_front = np.array(data[:, :4])
      else:
        data_front = np.concatenate((data_front, np.array(data[:, :4])))


      data_self = np.array(data[:, 4:])

      stat.loc['%d_self_amin' % i] = np.amin(data_self, axis=0)
      stat.loc['%d_self_ptp' % i] = np.ptp(data_self, axis=0)
      stat.loc['%d_self_mean' % i] = np.mean(data_self, axis=0)
      stat.loc['%d_self_std' % i] = np.std(data_self, axis=0)

      if i == 0:
        data_self_normal[db][table] = np.array((data_self - np.amin(data_self, axis=0)) / np.ptp(data_self, axis=0))
        data_self_zscore[db][table] = np.array((data_self - np.mean(data_self, axis=0)) / np.std(data_self, axis=0))
      else:
        data_self_normal[db][table] = np.concatenate((data_self_normal[db][table], np.array((data_self - np.amin(data_self, axis=0)) / np.ptp(data_self, axis=0))))
        data_self_zscore[db][table] = np.concatenate((data_self_zscore[db][table], np.array((data_self - np.mean(data_self, axis=0)) / np.std(data_self, axis=0))))


      data_b_front = data[::2, :4]
      data_r_front = data[1::2, :4]
      data_b_self = data[::2, 4:]
      data_r_self = data[1::2, 4:]

      data_b_diff = np.concatenate((data_b_front, data_b_self - data_r_self), axis=1)
      data_r_diff = np.concatenate((data_r_front, data_r_self - data_b_self), axis=1)

      data_diff = np.concatenate((data_b_diff, data_r_diff))
      data_diff = np.array(data_diff[data_diff[:, 0].argsort()][:, 4:])

      if i == 0:
        data_diff_normal[db][table] = np.array((data_diff - np.amin(data_diff, axis=0)) / np.ptp(data_diff, axis=0))
        data_diff_zscore[db][table] = np.array((data_diff - np.mean(data_diff, axis=0)) / np.std(data_diff, axis=0))
      else:
        data_diff_normal[db][table] = np.concatenate((data_diff_normal[db][table], np.array((data_diff - np.amin(data_diff, axis=0)) / np.ptp(data_diff, axis=0))))
        data_diff_zscore[db][table] = np.concatenate((data_diff_zscore[db][table], np.array((data_diff - np.mean(data_diff, axis=0)) / np.std(data_diff, axis=0))))

      stat.loc['%d_diff_amin' % i] = np.amin(data_diff, axis=0)
      stat.loc['%d_diff_ptp' % i] = np.ptp(data_diff, axis=0)
      stat.loc['%d_diff_mean' % i] = np.mean(data_diff, axis=0)
      stat.loc['%d_diff_std' % i] = np.std(data_diff, axis=0)

      stat.to_csv('dataset/s11/v01/%s/stat_%s.csv' % (db, table))


    np.save('dataset/s11/v01/%s/normal_self_%s' % (db, table), data_self_normal[db][table])
    np.save('dataset/s11/v01/%s/zscore_self_%s' % (db, table), data_self_zscore[db][table])
    np.save('dataset/s11/v01/%s/normal_diff_%s' % (db, table), data_diff_normal[db][table])
    np.save('dataset/s11/v01/%s/zscore_diff_%s' % (db, table), data_diff_zscore[db][table])


    if table == 'result':
      if db == 'top':
        for i in range(5):
          key[i] = np.logical_or(data_front[:, 0] % 10 == 2*i, data_front[:, 0] % 10 == 2*i+1)
          np.save('dataset/s11/v01/key_%d' % (i), key[i])
          np.save('dataset/s11/v01/Y_%d' % (i), data_front[key[i], 1])

      duration = data_front[:, 3]
      mean, std = np.mean(duration), np.std(duration)
      duration -= mean
      duration /= std

      pd.Series({'mean': mean, 'std': std}).to_csv('dataset/s11/v01/%s/stat_duration.csv' % (db), index=False)
     
      champion = data_front[:, 2]
      unique, counts = np.unique(champion, return_counts=True)
      champ_count = dict(zip(unique, counts))

      champ_list = [2000]
      for champ in champ_count:
        if champ_count[champ] >= len(data_front) / 400:
          champ_list.append(champ)

      for_onehot = [-i for i in range(len(champ_list))]
      random.shuffle(for_onehot)
      isin = np.isin(champion, champ_list)
      champ_out = np.where(isin, champion, 2000)

      for i in range(len(champ_list)):
        champ = champ_list[i]
        champ_out = np.where(champ_out==champ, for_onehot[i], champ_out)

      champ_out = np.array(np.abs(champ_out), dtype='int8')
      champ_onehot = np.eye(len(champ_list))[champ_out]
      
      data_front_save = np.array(np.concatenate((champ_onehot, duration.reshape((-1,1))), axis=1))

      np.save('dataset/s11/v01/%s/data_front' % (db), data_front_save)
      pd.DataFrame.from_dict([dict(zip(champ_list, for_onehot))]).to_csv('dataset/s11/v01/%s/champ_dict.csv' % (db), index=False)
