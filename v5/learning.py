from os import path
from google.colab import drive

notebooks_dir_name = 'notebooks'
drive.mount('/content/gdrive')
notebooks_base_dir = path.join('./gdrive/My Drive/', notebooks_dir_name)

import tensorflow as tf
import numpy as np
import random
import gc

from tensorflow.keras import optimizers, datasets, layers, models



train_x_npy = []
train_y_npy = []

for i in range(1,21):
  seed = [j for j in range(1,81)]
  random.shuffle(seed)

  rand_x = ['v%02d/train/data/p%02d.npy'%(i,rand) for rand in seed]
  rand_y = ['v%02d/train/label/p%02d.npy'%(i,rand) for rand in seed]
  
  
  train_x_npy.append(rand_x)
  train_y_npy.append(rand_y)
  
  
initial_learning_rate = 0.001
lr_schedule = optimizers.schedules.ExponentialDecay(
    initial_learning_rate,
    decay_steps=28000,
    decay_rate=0.96,
    staircase=True)

optimizer = tf.keras.optimizers.RMSprop(learning_rate=lr_schedule)

model = models.Sequential()

model.add(layers.Conv2D(32, (5, 5), activation='relu', padding='same', input_shape=(28, 28, 11)))
model.add(layers.MaxPooling2D((2, 2)))
model.add(layers.Conv2D(64, (5, 5), activation='relu', padding='same'))
model.add(layers.MaxPooling2D((2, 2)))
model.add(layers.Conv2D(128, (3, 3), activation='relu', padding='same'))

model.add(layers.Flatten())
model.add(layers.Dense(64, activation='relu'))
model.add(layers.Dense(2, activation='softmax'))


#model.compile(optimizer=optimizers.Adam(learning_rate=lr_schedule),
              
#model.compile(optimizer=optimizer,
model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy',
              metrics=['sparse_categorical_accuracy'])
              

addr = './gdrive/My Drive/notebooks/lol_hightier_minute_data_over_ten/'

for _ in range(1):
  for i in range(40):
    train_order = [j for j in range(20)]
    random.shuffle(train_order)

    for k in range(2):
      for j in range(20):
        train_data = np.array(np.load(addr+train_x_npy[train_order[j]][2*i+k]) / 255.0, dtype=np.float32)
        #train_data = np.load(addr+train_x_npy[train_order[j]][i])
        train_label = np.load(addr+train_y_npy[train_order[j]][2*i+k])

        if j+k == 0:
          train_x = train_data
          train_y = train_label
        else:
          train_x = np.append(train_x, train_data, axis=0)
          train_y = np.append(train_y, train_label, axis=0)
        
        gc.collect()


    p = np.random.permutation(len(train_x))
    train_x = train_x[p]
    train_y = train_y[p]
    
    gc.collect()

    
    print(i+1, 'th iteration')
    model.fit(train_x, train_y, verbose=2, epochs=5, batch_size=128)
    print()
    gc.collect()
    
    
accuracy = []

for j in range(1, 21):
  for k in range(1, 11):
    test_x_npy = 'v%02d/test/data/p%02d.npy'%(j,k)
    test_y_npy = 'v%02d/test/label/p%02d.npy'%(j,k)

    test_data = np.array(np.load(addr+test_x_npy) / 255.0, dtype=np.float32)
    test_label = np.load(addr+test_y_npy)

    if k == 1:
      test_x = test_data
      test_y = test_label
    else:
      test_x = np.append(test_x, test_data, axis=0)
      test_y = np.append(test_y, test_label, axis=0)

  test_loss, test_acc = model.evaluate(test_x, test_y, verbose=0)
  accuracy.append(test_acc)

  gc.collect()

print(accuracy)
  
