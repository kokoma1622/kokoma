import tensorflow as tf
import pandas as pd
import numpy as np
import psycopg2

from sqlalchemy import create_engine
from copy import deepcopy 

from tensorflow.keras import optimizers, datasets, layers, models

uri = 'postgresql://kokoma:qkr741963@lol-hightier-data.cyt1o4h42mbw.us-east-1.rds.amazonaws.com:5432/v10_24'
engine = create_engine(uri, echo=False)

table = pd.read_sql('select * from draven_normalize', con=engine).sample(frac=1)
column =  ['gold', 'damage_dealt_champion', 'damage_dealt_object', 'damage_dealt_tower', 'damage_taken', 'total_heal', 'damage_mitiagted', 
           'cs_minion', 'cs_jungle', 'cs_jungle_team', 'cs_jungle_enemy', 'vision_score', 'ward_bought', 'ward_placed', 'ward_killed']


X = table[column].to_numpy(copy=True)
Y = table['win'].to_numpy(copy=True)

X_train = X[:40000]
Y_train = Y[:40000]
X_test = X[40000:50000]
Y_test = Y[40000:50000]

X_ex = X[50000:]
Y_ex = Y[50000:]

def build_model():
  model = models.Sequential()

  model.add(layers.Dense(400, activation=tf.nn.relu6, input_shape=(14,)))
  model.add(layers.Dense(1000, activation=tf.nn.relu6))
  model.add(layers.Dense(200, activation=tf.nn.relu6))
  model.add(layers.Dense(50, activation=tf.nn.relu6))
  model.add(layers.Dense(2, activation='softmax'))

  return model
  
accuracy = []
for _ in range(10):
  accuracy_attempt = []
  
  model = build_model()
  model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])

  model.fit(X_train, Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_attempt.append(model.evaluate(X_test, Y_test, verbose=0)[1])
  
  for i in range(15):
    X_train_r = np.delete(X_train, i, axis=1)
    X_test_r = np.delete(X_test, i, axis=1)

    model = build_model()
    model.compile(optimizer='adam',
                loss='sparse_categorical_crossentropy',
                metrics=['sparse_categorical_accuracy'])
    
    model.fit(X_train_r, Y_train, verbose=0, epochs=10, batch_size=128)
    accuracy_attempt.append(model.evaluate(X_test_r, Y_test, verbose=0)[1])

  accuracy.append(accuracy_attempt)
