from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)


import numpy as np
import tensorflow as tf
from tensorflow.python.keras.layers import InputLayer, LSTM, Dense, Activation, TimeDistributed
from tensorflow.python.keras.models import Sequential 

db = 'top'

troll_X = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/troll_X.npy' % (db))
troll_Y = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/troll_Y.npy' % (db))
troll_len = np.load('./gdrive/My Drive/notebooks/s11/v02/%s/troll_gamelen.npy' % (db))

X_ragged = tf.RaggedTensor.from_row_lengths(troll_X, row_lengths=troll_len)
Y_ragged = tf.RaggedTensor.from_row_lengths(troll_Y, row_lengths=troll_len)

model = Sequential()

model.add(InputLayer(input_shape=[None, 3], ragged=True))
model.add(LSTM(8))
model.add(Dense(2, activation='softmax'))

lr_schedule = tf.keras.optimizers.schedules.ExponentialDecay(
    initial_learning_rate=0.002, decay_steps=5985, decay_rate=0.3, staircase=True)

callback = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=2)

model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=lr_schedule), 
              loss='sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])
                
Y_rag = [[0] if sum(np.array(Y_one))<=len(Y_one)/10 else [1] for Y_one in Y_train]

model.fit(X_train, np.array(Y_rag)[:train_len], epochs=30, batch_size=128, callbacks=[callback])

model.evaluate(X_test, np.array(Y_rag[train_len:]))
