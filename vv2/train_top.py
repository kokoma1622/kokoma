from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)

import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import optimizers, datasets, layers, models
import random

db = 'top'
table_list = ['result', 'detail', 'laning']

X_champion, X_duration, X_self, X_diff = {}, {}, {}, {}
Y, key, mod = {}, {}, {}

for i in range(5):
  X_champion[i] = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/champion_%d.npy' % (db, i))
  X_duration[i] = np.load('./gdrive/My Drive/notebooks/s11/v02/duration_%d.npy' % (i))
  Y[i] = np.load('./gdrive/My Drive/notebooks/s11/v02/Y_%d.npy' % (i))
  key[i] = np.load('./gdrive/My Drive/notebooks/s11/v02/key_%d.npy' % (i))
  mod[i] = np.load('./gdrive/My Drive/notebooks/s11/v02/mod_%d.npy' % (i))
  
for table in table_list:
  X_self[table] = {}
  X_diff[table] = {}
  for i in range(5):
    X_self[table][i] = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/%s_data_self_%d.npy' % (db, table, i))
    X_diff[table][i] = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/%s_data_diff_%d.npy' % (db, table, i))
    
X = {}

for i in range(5):
  X[i] = np.concatenate((X_champion[i], X_duration[i], X_self['result'][i], X_self['detail'][i], X_self['laning'][i], 
                         X_diff['result'][i], X_diff['detail'][i], X_diff['laning'][i]), axis=1)
                         
lr_schedule = tf.keras.optimizers.schedules.ExponentialDecay(
    initial_learning_rate=0.001, decay_steps=72279, decay_rate=0.3, staircase=True)

callback = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)

def build_model(input_size):
  model = models.Sequential()

  model.add(layers.Dense(500, activation=tf.sigmoid, input_shape=(input_size,)))
  model.add(layers.Dense(100, activation=tf.sigmoid))
  model.add(layers.Dense(2, activation='softmax'))

  model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr_schedule),
                loss='sparse_categorical_crossentropy',
                metrics=['sparse_categorical_accuracy'])
  
  return model
  
train_order = [[0,1,2], [1,2,3], [2,3,4], [3,4,0], [4,0,1]]
valid_order = [3, 4, 0, 1, 2]
test_order = [4, 0, 1, 2, 3]

acc, ratio = {}, {}

for i in range(1):
  name = 'test_%d' % (test_order[i])

  X_train = np.concatenate((X[train_order[i][0]], X[train_order[i][1]], X[train_order[i][2]]))
  Y_train = np.concatenate((Y[train_order[i][0]], Y[train_order[i][1]], Y[train_order[i][2]]))
  X_valid = X[valid_order[i]]
  Y_valid = Y[valid_order[i]]
  X_test = X[test_order[i]]
  Y_test = Y[test_order[i]]

  model = build_model(len(X_train[0]))
  model.fit(X_train, Y_train, verbose=0, validation_data=(X_valid, Y_valid), epochs=40, batch_size=256, callbacks=[callback])
  acc[name] = model.evaluate(X_test, Y_test, verbose=0)[1]

  predict = np.sort(model.predict(X_test)[:, 1])
  ratio[name] = []
  for j in range(1000):
    ratio[name].append(predict[int(j*len(X_test)/1000)])
  ratio[name].append(predict[-1])

  print(name)
  print(acc[name])
  
  model.save('./gdrive/My Drive/notebooks/s11/v02/%s/test_%d' % (db, test_order[i]))
  np.save('./gdrive/My Drive/notebooks/s11/v02/%s/test_%d/ratio' % (db, test_order[i]), ratio[name])

print(acc)
