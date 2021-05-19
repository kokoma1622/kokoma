from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)


import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import LSTM, Dense 
from tensorflow.keras.models import Sequential 

db_list = ['top', 'jgl', 'mid', 'bot', 'sup']

lr_schedule = tf.keras.optimizers.schedules.ExponentialDecay(
    initial_learning_rate=0.001, decay_steps=26712, decay_rate=0.3, staircase=True)

callback = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=2)

def build_rnn():
  model = Sequential()
  model.add(LSTM(10, input_shape=(5, 3)))
  model.add(Dense(2, activation='softmax')) 
  model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr_schedule), 
                loss='sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])
  
  return model

model = {}


for db in db_list:
  troll_X = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/troll_minute_X.npy' % (db))
  troll_Y = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/troll_minute_Y.npy' % (db))
  length = len(troll_Y)

  train_index = np.array([False if i % 4 == 0 else True for i in range(length)])
  test_index = np.array([True if i % 4 == 0 else False for i in range(length)])

  model[db] = build_rnn()
  model[db].fit(troll_X[train_index], troll_Y[train_index], epochs=100,
            batch_size=256, verbose=1, callbacks=[callback])
  
  print(model[db].evaluate(troll_X[test_index], troll_Y[test_index], batch_size=256))
