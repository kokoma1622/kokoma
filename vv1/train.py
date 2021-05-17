import numpy as np
import tensorflow as tf
from tensorflow.keras import optimizers, datasets, layers, models


def build_model(input_size):
  lr_schedule = tf.keras.optimizers.schedules.ExponentialDecay(
      initial_learning_rate=0.001, decay_steps=36000, decay_rate=0.2, staircase=True)


  model = models.Sequential()

  model.add(layers.Dense(200, activation=tf.nn.relu6, input_shape=(input_size,)))
  model.add(layers.Dense(50, activation=tf.nn.relu6))
  model.add(layers.Dense(2, activation='softmax'))

  model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr_schedule),
                loss='sparse_categorical_crossentropy',
                metrics=['sparse_categorical_accuracy'])

  return model


def get_data():
  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  table_list = ['result', 'detail', 'laning']

  data_front = {}
  data_self_normal, data_self_zscore, data_diff_normal, data_diff_zscore = {}, {}, {}, {}
  for db in db_list:
    data_front[db] = np.load('dataset/s11/v01/%s/data_front.npy' % (db))

    data_self_normal[db], data_self_zscore[db], data_diff_normal[db], data_diff_zscore[db] = {}, {}, {}, {}
    for table in table_list:
      #data_self_normal[db][table] = np.load('dataset/s11/v01/%s/normal_self_%s.npy' % (db, table))
      data_self_zscore[db][table] = np.load('dataset/s11/v01/%s/zscore_self_%s.npy' % (db, table))
      #data_diff_normal[db][table] = np.load('dataset/s11/v01/%s/normal_diff_%s.npy' % (db, table))
      data_diff_zscore[db][table] = np.load('dataset/s11/v01/%s/zscore_diff_%s.npy' % (db, table))

  return data_front, data_self_normal, data_self_zscore, data_diff_normal, data_diff_zscore


def get_xy(data_front, data_self_normal, data_self_zscore, data_diff_normal, data_diff_zscore):
  Y, key = {}, {}
  for i in range(5):
    Y[i] = np.load('dataset/s11/v01/Y/Y_%d.npy' % (i))
    key[i] = np.load('dataset/s11/v01/key/key_%d.npy' % (i))

  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']

  X = {'cddd': {}, 'ddd': {}}
  for db in db_list:
    #X_data_normal = np.concatenate((data_front[db], data_self_normal[db]['result'], data_self_normal[db]['detail'], data_self_normal[db]['laning'],
    #                                                data_diff_normal[db]['result'], data_diff_normal[db]['detail'], data_diff_normal[db]['laning']), axis=1)
    #X['normal'][db] = [np.copy(X_data_normal[key[i]]) for i in range(5)]
    #

    #X_data_zscore = np.concatenate((data_front[db], data_self_zscore[db]['result'], data_self_zscore[db]['detail'], data_self_zscore[db]['laning'],
    #                                                data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'], data_diff_zscore[db]['laning']), axis=1)

    #X_data_zscore = np.concatenate((data_self_zscore[db]['result'], data_self_zscore[db]['detail'], data_self_zscore[db]['laning'],
    #                                data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'], data_diff_zscore[db]['laning']), axis=1)
    #X['sssddd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

    #X_data_zscore = np.concatenate((data_front[db], data_self_zscore[db]['result'], data_self_zscore[db]['detail'], data_self_zscore[db]['laning']), axis=1)
    #X['csss'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

    X_data_zscore = np.concatenate((data_front[db], data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'], data_diff_zscore[db]['laning']), axis=1)
    X['cddd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

    X_data_zscore = np.concatenate((data_diff_zscore[db]['result'], data_diff_zscore[db]['detail'], data_diff_zscore[db]['laning']), axis=1)
    X['ddd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

    #X_data_zscore = np.concatenate((data_front[db], data_self_zscore[db]['result'], data_self_zscore[db]['detail'],
    #                                                data_diff_zscore[db]['result'], data_diff_zscore[db]['detail']), axis=1)
    #X['cssdd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

    #X_data_zscore = np.concatenate((data_self_zscore[db]['result'], data_self_zscore[db]['detail'],
    #                                data_diff_zscore[db]['result'], data_diff_zscore[db]['detail']), axis=1)
    #X['ssdd'][db] = [np.copy(X_data_zscore[key[i]]) for i in range(5)]

  return X, Y



def main():
  data_front, data_self_normal, data_self_zscore, data_diff_normal, data_diff_zscore = get_data()
  X, Y = get_xy(data_front, data_self_normal, data_self_zscore, data_diff_normal, data_diff_zscore)


  train_order = [[0,1,2], [0,1,2], [0,1,3], [0,1,3], [0,1,4], [0,1,4], [0,2,3], [0,2,3], [0,2,4], [0,2,4],
                 [0,3,4], [0,3,4], [1,2,3], [1,2,3], [1,2,4], [1,2,4], [1,3,4], [1,3,4], [2,3,4], [2,3,4]]
  valid_order = [3, 4, 2, 4, 2, 3, 1, 4, 1, 3, 1, 2, 0, 4, 0, 3, 0, 2, 0, 1]
  test_order = [4, 3, 4, 2, 3, 2, 4, 1, 3, 1, 2, 1, 4, 0, 3, 0, 2, 0, 1, 0]


  callback = tf.keras.callbacks.EarlyStopping(monitor='val_loss', mode='min', patience=3)

  acc = {}
  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  #method_list = ['sssddd', 'ssdd']
  method_list = ['cddd', 'ddd']

  for db in db_list:
    for method in method_list:
      how = db + '_' + method
      acc[how] = []
      #for i in range(20):
      for i in range(2):
        X_train = np.concatenate((X[method][db][train_order[i][0]], X[method][db][train_order[i][1]], X[method][db][train_order[i][2]]))
        Y_train = np.concatenate((Y[train_order[i][0]], Y[train_order[i][1]], Y[train_order[i][2]]))
        X_valid = X[method][db][valid_order[i]]
        Y_valid = Y[valid_order[i]]
        X_test = X[method][db][test_order[i]]
        Y_test = Y[test_order[i]]

        model = build_model(X[method][db][0].shape[1])
        model.fit(X_train, Y_train, verbose=0, validation_data=(X_valid, Y_valid), epochs=40, batch_size=512, callbacks=[callback])
        acc[how].append(model.evaluate(X_test, Y_test, verbose=0)[1])
        model.save('model/%s/%s/model_%d%d%d_%d_%d' % (method, db, train_order[i][0], train_order[i][1], train_order[i][2], valid_order[i], test_order[i]))
        #model.save('model/%s/%s_%d%d%d_%d_%d' % (method, db, train_order[i][0], train_order[i][1], train_order[i][2], valid_order[i], test_order[i]))

      print(how)
      print(acc[how])
      print(sum(acc[how])/20)


  #for how in acc:
  #  print(how, sum(acc[how])/20)



if __name__ == "__main__":
  main()
