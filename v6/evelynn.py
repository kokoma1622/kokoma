import tensorflow as tf
import pandas as pd
import numpy as np
import psycopg2

from sqlalchemy import create_engine
from copy import deepcopy 

from tensorflow.keras import optimizers, datasets, layers, models

uri = 'postgresql://kokoma:qkr741963@lol-hightier-data.cyt1o4h42mbw.us-east-1.rds.amazonaws.com:5432/v10_24'
engine = create_engine(uri, echo=False)

column =  ['kda', 'contribution', 'death_ratio', 'gold_pm', 'dmg_dealt_champion_pm', 'dmg_dealt_object_pm', 'dmg_dealt_tower_pm', 'dmg_taken_actual_pm', 'dmg_taken_prereduced_pm', 
           'total_healt_pm', 'cs_total_pm', 'cs_enemyjungle_pm', 'vision_score_pm', 'ward_bought_pm', 'ward_placed_pm', 'ward_killed_pm', 'dmg_dealt_champion_pd',  'dmg_dealt_tower_pm', 
           'dmg_taken_actual_pm', 'dmg_taken_prereduced_pm', 'kills_assists_14min', 'deaths_14min', 'level_14min', 'gold_14min', 'cs_14min']

table_self = pd.read_sql('select * from jgl_028_data_normalized_self order by gameid', con=engine).to_numpy()
table_diff = pd.read_sql('select * from jgl_028_data_normalized_diff order by gameid', con=engine).to_numpy()[:, 3:]
table = np.concatenate((table_self, table_diff), axis=1)
np.random.shuffle(table)

X = table[:, 3:]
Y = table[:, 2]

def build_model():
  model = models.Sequential()

  model.add(layers.Dense(400, activation=tf.nn.relu6, input_shape=(14,)))
  model.add(layers.Dense(1000, activation=tf.nn.relu6))
  model.add(layers.Dense(200, activation=tf.nn.relu6))
  model.add(layers.Dense(50, activation=tf.nn.relu6))
  model.add(layers.Dense(2, activation='softmax'))

  return model
  
X_train = X[:50000]
Y_train = Y[:50000]
X_test = X[50000:60000]
Y_test = Y[50000:60000]

X_ex = X[60000:]
Y_ex = Y[60000:]


def build_model1():
  model = models.Sequential()
  model.add(layers.Dense(100, activation=tf.nn.relu6, input_shape=(50,)))
  model.add(layers.Dense(1000, activation=tf.nn.relu6))
  model.add(layers.Dense(200, activation=tf.nn.relu6))
  model.add(layers.Dense(50, activation=tf.nn.relu6))
  model.add(layers.Dense(2, activation='softmax'))

  return model
 
 
def build_model2():
  model = models.Sequential()
  model.add(layers.Dense(300, activation=tf.nn.relu6, input_shape=(25,)))
  model.add(layers.Dense(3000, activation=tf.nn.relu6))
  model.add(layers.Dense(300, activation=tf.nn.relu6))
  model.add(layers.Dense(50, activation=tf.nn.relu6))
  model.add(layers.Dense(2, activation='softmax'))

  return model
  
  
accuracy_entire = 0
for _ in range(20):
  model = build_model()
  model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model.fit(X_train, Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_entire += round(model.evaluate(X_test, Y_test, verbose=0)[1], 4)
print(accuracy_entire/20)

accuracy_entire2 = 0
for _ in range(20):
  model2 = build_model2()
  model2.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model2.fit(X_train, Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_entire2 += round(model2.evaluate(X_test, Y_test, verbose=0)[1], 4)
print(accuracy_entire2/20)


accuracy_self1 = 0
for _ in range(20):
  model_self1 = build_model1()
  model_self1.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model_self1.fit(X_train[:, :25], Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_self1 += round(model_self1.evaluate(X_test[:, :25], Y_test, verbose=0)[1], 4)
print(accuracy_self1/20)


accuracy_self2 = 0
for _ in range(20):
  model_self2 = build_model2()
  model_self2.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model_self2.fit(X_train[:, :25], Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_self2 += round(model_self2.evaluate(X_test[:, :25], Y_test, verbose=0)[1], 4)
print(accuracy_self2/20)


accuracy_diff1 = 0
for _ in range(20):
  model_diff1 = build_model1()
  model_diff1.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model_diff1.fit(X_train[:, 25:], Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_diff1 += round(model_diff1.evaluate(X_test[:, 25:], Y_test, verbose=0)[1], 4)
print(accuracy_diff1/20)


accuracy_diff2 = 0
for _ in range(20):
  model_diff2 = build_model2()
  model_diff2.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
  model_diff2.fit(X_train[:, 25:], Y_train, verbose=0, epochs=10, batch_size=128)
  accuracy_diff2 += round(model_diff2.evaluate(X_test[:, 25:], Y_test, verbose=0)[1], 4)
print(accuracy_diff2/20)
