from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)

import tensorflow
import pandas as pd
import numpy as np
import psycopg2
import random
import time
from IPython.core.display import display
from sqlalchemy import create_engine
from PIL import Image
from copy import deepcopy 

remove_tier = ['IRON IV', 'IRON III', 'IRON II', 'IRON I', 'BRONZE IV', 'BRONZE III', 'BRONZE II', 'BRONZE I', 'SILVER IV', 'SILVER III', 'SILVER II', 'SILVER I', 
               'GOLD IV', 'GOLD III', 'GOLD II', 'GOLD I', 'PLATINUM IV', 'PLATINUM III', 'PLATINUM II', 'PLATINUM I']

champ = [266, 103, 84, 12, 32, 34, 1, 523, 22, 136, 268, 432, 53, 63, 201, 51, 164, 69, 31, 42, 122, 131, 36, 119, 245, 60, 28, 81, 9, 114, 105, 3, 41, 86, 150, 79, 104, 
         120, 74, 420, 39, 427, 40, 59, 24, 126, 202, 222, 145, 429, 43, 30, 38, 55, 10, 141, 85, 121, 203, 240, 96, 7, 64, 89, 876, 127, 236, 117, 99, 54, 90, 57, 11, 
         21, 62, 82, 25, 267, 75, 111, 518, 76, 56, 20, 2, 61, 516, 80, 78, 555, 246, 133, 497, 33, 421, 58, 107, 92, 68, 13, 360, 113, 235, 875, 35, 98, 102, 27, 14, 15, 
         72, 37, 16, 50, 517, 134, 223, 163, 91, 44, 17, 412, 18, 48, 23, 4, 29, 77, 6, 110, 67, 45, 161, 254, 112, 8, 106, 19, 498, 101, 5, 157, 777, 83, 350, 154, 238, 
         115, 26, 142, 143]

seed = [i for i in range(10,161)]
random.shuffle(seed)

champ_random = dict(zip(champ, seed))

data_columns = ['cs', 'level', 'kills', 'deaths', 'assists']
column_convert = {'cs':4, 'level':6, 'kills':8, 'deaths':10, 'assists':12}

for version in range(1,20):
  if version < 13:
    uri = 'postgresql://kokoma:qkr741963@lol-hightier-data.cnvgsj5mbcjf.us-east-1.rds.amazonaws.com:5432/v10_%02d' % version
  else:
    uri = 'postgresql://kokoma:qkr741963@lol-hightier-data.cyt1o4h42mbw.us-east-1.rds.amazonaws.com:5432/v10_%02d' % version
  engine = create_engine(uri, echo=False)

  if version != 1:
    game = pd.read_sql("select gameid, tier, duration, win from game_summary", con=engine)
    user_champ = pd.read_sql("select gameid, userid, role, champion from personal_summary", con=engine)
    user_minute = pd.read_sql("select gameid, minute, level, kills, deaths, assists, cs from minute_personal", con=engine)

  for tier in remove_tier:
    game = game[game['tier'] != tier]

  game = game[game['duration'] > 300]

  gameid_list = game['gameid'].tolist()
  win_list = game['win'].tolist()


  dataset_x = []
  dataset_y = []
  start = time.time()
  train_num, val_num, test_num = 1, 1, 1

  for i in range(35000):
    gameid = gameid_list[i]
    
    champion = user_champ[user_champ['gameid'] == gameid].sort_values(by='userid')['champion'].tolist()
    role = user_champ[user_champ['gameid'] == gameid].sort_values(by='userid')['role'].tolist()
    user_data = user_minute[user_minute['gameid'] == gameid]
    duration = len(user_data)
    
    image = np.zeros((duration-1, 14, 14, 11), dtype=np.int8)

    for member in range(10):
      color = champ_random[champion[member]]
      if member < 5:
        image[:, ::2, :, member] = color
      else:
        image[:, 1::2, :, member] = color

    if (version-1) // 12 == 0:
      image[:, 1:(version-1)%12+2, 1, :10] = 255
    else:
      image[:, 1:13, 1, :10] = 255
      image[:, 1:(version-1)%12+2, 2, :10] = 255

    if win_list[i] == 'BLUE':
      answer = np.zeros((duration-1), dtype=np.int8)
    else:
      answer = np.ones((duration-1), dtype=np.int8)


    for minute in range(1,duration):
      image[minute-1, :, :, 10] = minute * 2

      minute_data = user_data[user_data['minute'] == minute]

      for column in data_columns:
        column_data = minute_data[column].tolist()[0]
      
        if column == 'cs':
          for member in range(10):
            cs_norm = max(min(int((column_data[member] / minute - 3) * 1.5), 12), 0)

            image[minute-1, 1:1+cs_norm, column_convert[column], member] = 255


        else:
          column_max = max(column_data)
          column_min = min(column_data) - 1

          if column_max == column_min + 1:
            image[minute-1, 1:7, column_convert[column], :10] = 255
            continue

          column_gap = column_max - column_min

          for member in range(10):
            column_norm = int((column_data[member] - column_min) / column_gap * 12)

            image[minute-1, 1:1+column_norm, column_convert[column], member] = 255

    
    if len(dataset_x) == 0:
      dataset_x = np.copy(image)
      dataset_y = np.copy(answer)
    else:
      dataset_x = np.append(dataset_x, image, axis=0)
      dataset_y = np.append(dataset_y, answer)
    

    if i % 1000 == 999:
      if i % 7000 == 5999:
        name_x = './gdrive/My Drive/notebooks/v%02d/val/data/p%02d' % (version, val_num)
        name_y = './gdrive/My Drive/notebooks/v%02d/val/label/p%02d' % (version, val_num)
        val_num += 1
      elif i % 7000 == 6999:
        name_x = './gdrive/My Drive/notebooks/v%02d/test/data/p%02d' % (version, test_num)
        name_y = './gdrive/My Drive/notebooks/v%02d/test/label/p%02d' % (version, test_num)
        test_num += 1
      else:
        name_x = './gdrive/My Drive/notebooks/v%02d/train/data/p%02d' % (version, train_num)
        name_y = './gdrive/My Drive/notebooks/v%02d/train/label/p%02d' % (version, train_num)
        train_num += 1

      np.save(name_x, dataset_x)
      np.save(name_y, dataset_y)
    
      dataset_x = []
      dataset_y = []
