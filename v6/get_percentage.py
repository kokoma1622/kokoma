import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import optimizers, datasets, layers, models


def get_data():
  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  table_list = ['result', 'detail', 'laning']

  data_front = {}
  data_self_zscore, data_diff_zscore = {}, {}
  for db in db_list:
    data_front[db] = np.load('dataset/s11/v01/%s/data_front.npy' % (db))

    data_self_zscore[db], data_diff_zscore[db] = {}, {}
    for table in table_list:
      data_self_zscore[db][table] = np.load('dataset/s11/v01/%s/zscore_self_%s.npy' % (db, table))
      data_diff_zscore[db][table] = np.load('dataset/s11/v01/%s/zscore_diff_%s.npy' % (db, table))

  return data_front, data_self_zscore, data_diff_zscore



def get_xy(data_front, data_self_zscore, data_diff_zscore):
  Y, key = {}, {}
  for i in range(5):
    Y[i] = np.load('dataset/s11/v01/Y/Y_%d.npy' % (i))
    key[i] = np.load('dataset/s11/v01/key/key_%d.npy' % (i))

  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']

  X = {'cddd': {}, 'ddd': {}}
  for db in db_list:
    X_data_zscore = np.concatenate((data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'], data_diff_zscore[db]['laning']), axis=1)
    X['ddd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

  """
  X = {'small': {}}
  for db in db_list:
    X_data_zscore = np.concatenate((data_self_zscore[db]['result'], data_self_zscore[db]['detail'][:, [0,3,6,7]], data_self_zscore[db]['laning'][:, [1,2,3,4]],
                                    data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'][:, [0,3,6,7]], data_diff_zscore[db]['laning'][:, [1,2,3,4]]), axis=1)
    X['small'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]
  """
  return X, Y



def main():
  data_front, data_self_zscore, data_diff_zscore = get_data()
  X, Y = get_xy(data_front, data_self_zscore, data_diff_zscore)

  train_order = [[0,1,2], [0,1,2], [0,1,3], [0,1,3], [0,1,4], [0,1,4], [0,2,3], [0,2,3], [0,2,4], [0,2,4],
                 [0,3,4], [0,3,4], [1,2,3], [1,2,3], [1,2,4], [1,2,4], [1,3,4], [1,3,4], [2,3,4], [2,3,4]]
  valid_order = [3, 4, 2, 4, 2, 3, 1, 4, 1, 3, 1, 2, 0, 4, 0, 3, 0, 2, 0, 1]
  test_order = [4, 3, 4, 2, 3, 2, 4, 1, 3, 1, 2, 1, 4, 0, 3, 0, 2, 0, 1, 0]

  #model_name_list = ['model_%d%d%d_%d_%d' % (train_order[i][0], train_order[i][1], train_order[i][2], valid_order[i], test_order[i]) for i in range(20)]
  model_name_list = ['model_%d%d%d_%d_%d' % (train_order[i][0], train_order[i][1], train_order[i][2], valid_order[i], test_order[i]) for i in range(20)]
  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  method_list = ['ddd']
  #method_list = ['small']

  for method in method_list:
    for db in db_list:
      #predict_df = pd.DataFrame(columns=model_name_list, index=[i for i in range(1001)])
      predict_df = pd.DataFrame(columns=db_list, index=[i for i in range(1001)])
      for i in range(2):
        model_name = model_name_list[i]
        model_addr = 'model/%s/%s/' % (method, db)
        model = models.load_model(model_addr+model_name)

        X_test = X[method][db][test_order[i]]
        predict = np.sort(model.predict(X_test)[:, 1])
        size = len(predict)
        predict_df[model_name] = [predict[int(j*size/1000)] for j in range(1000)] + [float(predict[-1])]

      predict_df.to_csv('model/%s/%s/percent.csv' % (method, db))
      print(method, db)

if __name__ == "__main__":
  main()
