from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)


import numpy as np


db_list = ['top', 'jgl', 'mid', 'bot', 'sup']

for db in db_list:
  for i in range(1, 6):
    if i == 1:
      minute_data = np.load('./gdrive/My Drive/notebooks/s11/v02/troll/%s_minute_%d.npy' % (db, i))
    else:
      minute_data = np.concatenate((minute_data, np.load('./gdrive/My Drive/notebooks/s11/v02/troll/%s_minute_%d.npy' % (db, i))))

  gameid_list = list(set(minute_data[:, 0].tolist()))
  gap = {'top': np.array([2300, 1750, 30]), 'jgl': np.array([1830, 1820, 27]), 'mid': np.array([2280, 1790, 31]), 
         'bot': np.array([1660, 1840, 34]), 'sup': np.array([1420, 1350, 5.4])}


  X, Y = [], []

  for gameid in gameid_list:
    one_game = minute_data[minute_data[:, 0]==gameid, 1:]
    one_game = one_game[:-1, :]
    one_game[:, 2] -= 500
    length = len(one_game)

    for i in range(length-5):
      min5_X = []
      for j in range(1, 6):
        min5_X.append(np.array((one_game[i+j, 1:] - one_game[i, 1:]) / gap[db], dtype='float32').tolist())
      
      X.append(min5_X)
      Y.append(get_Y(min5_X))

    if (gameid_list.index(gameid)+1) % (len(gameid_list) // 10) == 0:
      print((gameid_list.index(gameid)) // (len(gameid_list) // 10))

  np.save('./gdrive/My Drive/notebooks/s11/v02/%s/troll_minute_X' % (db), np.array(X))
  np.save('./gdrive/My Drive/notebooks/s11/v02/%s/troll_minute_Y' % (db), np.array(Y))
