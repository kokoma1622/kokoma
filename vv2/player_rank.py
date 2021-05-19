from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)


import numpy as np
import pandas as pd
import requests


duration = np.load('./gdrive/My Drive/notebooks/s11/v02/duration_0.npy')
duration_stat = np.load('./gdrive/My Drive/notebooks/s11/v02/duration_stat.npy')
duration = np.array(duration * duration_stat[1] + duration_stat[0], dtype='int32')

gap = [0, 1189, 1322, 1447, 1565, 1669, 1773, 1883, 2013, 2197, 8000]
interval = {}

for i in range(10):
  interval[i] = np.logical_and(duration>=gap[i], duration<gap[i+1]).T[0]
 
 
db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
table_list = ['result', 'detail', 'laning']
entire, real = {}, {}
stat_avg, stat_std = {}, {}


for db in db_list:
  real[db] = {}
  for table in table_list:
    data = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/%s_data_self_0.npy' % (db, table))
    data_avg = pd.read_csv('./gdrive/My Drive/notebooks/s11/v02/%s/%s_stat_self_avg.csv' % (db, table), index_col=0)
    data_std = pd.read_csv('./gdrive/My Drive/notebooks/s11/v02/%s/%s_stat_self_std.csv' % (db, table), index_col=0)
  
    if table == 'result':
      stat_avg[db] = data_avg
      stat_std[db] = data_std

      avg = np.array(interval[0] * data_avg.loc[0, 'gold_pm'])
      std = np.array(interval[0] * data_std.loc[0, 'gold_pm'])
      for i in range(1, 10):
        avg += interval[i] * data_avg.loc[i, 'gold_pm']
        std += interval[i] * data_std.loc[i, 'gold_pm']
      real[db]['gold'] = np.array(data[:, 3] * std + avg, dtype='int32')
      entire[db] = np.array(data)
    
    else:
      if table == 'detail':
        stat_avg[db] = pd.concat([stat_avg[db], data_avg], axis=1)
        stat_std[db] = pd.concat([stat_std[db], data_std], axis=1)

        avg = np.array(interval[0] * data_avg.loc[0, 'dmg_dealt_champ_pm'])
        std = np.array(interval[0] * data_std.loc[0, 'dmg_dealt_champ_pm'])
        for i in range(1, 10):
          avg += interval[i] * data_avg.loc[i, 'dmg_dealt_champ_pm']
          std += interval[i] * data_std.loc[i, 'dmg_dealt_champ_pm']
        real[db]['dmg_d'] = np.array(data[:, 0] * std + avg, dtype='int32')

        avg = np.array(interval[0] * data_avg.loc[0, 'dmg_taken_actual_pm'])
        std = np.array(interval[0] * data_std.loc[0, 'dmg_taken_actual_pm'])
        for i in range(1, 10):
          avg += interval[i] * data_avg.loc[i, 'dmg_taken_actual_pm']
          std += interval[i] * data_std.loc[i, 'dmg_taken_actual_pm']
        real[db]['dmg_t'] = np.array(data[:, 3] * std + avg, dtype='int32')
      
      else:
        data_diff = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/%s_data_diff_0.npy' % (db, table))
        data = np.concatenate((data[:, 1:4], data_diff[:, 1:4]), axis=1)
        
        stat_avg[db] = pd.concat([stat_avg[db], data_avg.loc[:, ['xp_14min', 'gold_14min', 'cs_14min']]], axis=1)
        stat_std[db] = pd.concat([stat_std[db], data_std.loc[:, ['xp_14min', 'gold_14min', 'cs_14min']]], axis=1)

        data_std_diff = pd.read_csv('./gdrive/My Drive/notebooks/s11/v02/%s/%s_stat_diff_std.csv' % (db, table), index_col=0).loc[:, ['xp_14min', 'gold_14min', 'cs_14min']]
        stat_avg[db] = pd.concat([stat_avg[db], pd.DataFrame([[0,0,0]]*10)], axis=1)
        stat_std[db] = pd.concat([stat_std[db], data_std_diff], axis=1)
      
      entire[db] = np.concatenate((entire[db], data), axis=1)
      
column_list = ['gold', 'dmg_d', 'dmg_t']
share = {'top': {}, 'jgl': {}, 'mid': {}, 'bot': {}, 'sup': {}}

for column in column_list:
  for db in db_list:
    if db == 'top':
      data = np.array(real[db][column])
    else:
      data += real[db][column]

  for db in db_list:
    share[db][column] = real[db][column] / data
    
    avg_list, std_list = [], []
    for i in range(10):
      avg_list.append(np.mean(share[db][column][interval[i]]))
      std_list.append(np.std(share[db][column][interval[i]]))
    stat_avg[db][column] = avg_list
    stat_std[db][column] = std_list

    avg = np.array(interval[0] * avg_list[0])
    std = np.array(interval[0] * std_list[0])
    for i in range(1, 10):
      avg += interval[i] * avg_list[i]
      std += interval[i] * std_list[i]

    share[db][column] = (share[db][column] - avg) / std
    entire[db] = np.concatenate((entire[db], share[db][column].reshape(-1,1)), axis=1)
    
for db in db_list:
  entire[db] = np.maximum(np.minimum(entire[db], np.full_like(entire[db], 3)), np.full_like(entire[db], -3))
  
for db in db_list:    
  stat_avg[db] = stat_avg[db].to_numpy()
  stat_std[db] = stat_std[db].to_numpy()


ability, percent = {}, {}
abil_sum = {}
abil_list = ['laning', 'vision', 'growth', 'combat']
abil_item = {'laning': [19, 20, 21, 22, 23, 24], 'vision': [15, 16, 17, 18], 'growth': [3, 10, 25], 'combat': [0, 1, 4, 7, 26, 27]}
abil_weight = {'laning': [1, 1, 1, 1, 1, 1], 'vision': [3, 1, 1, 1], 'growth': [1, 1, 1], 'combat': [1, 1, 2, 2, 1, 1]}

for db in db_list:
  ability[db], percent[db] = {}, {}
  
  for abil in abil_list:
    ability[db][abil] = entire[db][:, abil_item[abil]]
    percent[db][abil] = []
    
    if abil == 'laning':
      abil_sum[db] = np.sum(ability[db][abil] * abil_weight[abil], axis=1).reshape(-1,1)
    else:
      abil_sum[db] = np.concatenate((abil_sum[db], np.sum(ability[db][abil] * abil_weight[abil], axis=1).reshape(-1,1)), axis=1)
      
      
abil_sort, abil_rank, percent = {}, {}, {}

for db in db_list:
  abil_sort[db], percent[db] = {}, {}
  abil_rank[db] = np.zeros((2055810, 4))
  for abil in abil_list:
    index = abil_list.index(abil)
    abil_sort[db][abil] = np.sort(abil_sum[db][:, index])
    abil_rank[db][:, index] = abil_sum[db][:, index].argsort().argsort() / 2055.81
    percent[db][abil] = [abil_sort[db][abil][int(2055.81*i)] for i in range(1000)] + [abil_sort[db][abil][-1]]
  
  abil_sort[db]['entire'] = np.sort(np.sum(abil_rank[db], axis=1))
  percent[db]['entire'] = [abil_sort[db]['entire'][int(2055.81*i)] for i in range(1000)] + [abil_sort[db]['entire'][-1]]
