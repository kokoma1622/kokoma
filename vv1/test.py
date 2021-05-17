import numpy as np
import pandas as pd
import tensorflow as tf
import requests
from tensorflow.keras import optimizers, datasets, layers, models
from copy import deepcopy


def is_where(coordinate):
    where = 'JUNGLE'
    difference = coordinate['x'] - coordinate['y']
    if coordinate['x'] < 3000 and coordinate['y'] < 3000:     where = 'BLUE HOME'
    elif coordinate['x'] > 12000 and coordinate['y'] > 12000: where = 'RED HOME'
    elif coordinate['y'] > 12000 or coordinate['x'] < 3000:   where = 'TOP'
    elif coordinate['x'] > 12000 or coordinate['y'] < 3000:   where = 'BOTTOM'
    elif -1500 < difference < 1500:                           where = 'MID'

    return where


def position_check(spells_list, items_list, frame):
    position = [-1 for _ in range(10)]

    blue_is_smite = [row.count(11) for row in spells_list[:5]]
    red_is_smite = [row.count(11) for row in spells_list[5:]]
    if sum(blue_is_smite) != 1 or sum(red_is_smite) != 1:
        return -1
    position[1] = blue_is_smite.index(1)
    position[6] = red_is_smite.index(1)+5

    support_item = [3850, 3851, 3853, 3862, 3863, 3864, 3858, 3859, 3860, 3854, 3855, 3857]
    blue_is_support = [sum(item in support_item for item in items) for items in items_list[:5]]
    red_is_support = [sum(item in support_item for item in items) for items in items_list[5:]]
    if sum(blue_is_support) != 1 or sum(red_is_support) != 1:
        return -1
    position[4] = blue_is_support.index(1)
    position[9] = red_is_support.index(1)+5


    position_determined = [position[i] for i in range(10) if position[i] != -1]
    for j in range(2):
        position_left = ['TOP', 'MID', 'BOTTOM']
        position_index = {'TOP': j*5, 'MID': j*5+2, 'BOTTOM':j*5+3}
        for i in range(5):
            userid = frame[0]['participantFrames'][str(j*5+i+1)]['participantId'] - 1
            if userid in position_determined:
                continue

            where_list = [is_where(frame[minute]['participantFrames'][str(j*5+i+1)]['position']) for minute in range(1, 15)]
            where = max(set(where_list), key = where_list.count)
            if where not in position_left:
                return -1

            position[position_index[where]] = userid
            position_left.remove(where)

    return position



def get_match(data_dict, time_dict):
    user = data_dict['participants']
    team = data_dict['teams']


    win_list = [1, 0] if team[0]['win'] == 'Win' else [0, 1]
    duration = data_dict['gameDuration']

    picks_list = [user[i]['championId'] for i in range(10)]

    spells_list = [[user[i]['spell1Id'], user[i]['spell2Id']] for i in range(10)]
    items_list = [[user[i]['stats']['item0'], user[i]['stats']['item1'], user[i]['stats']['item2'], user[i]['stats']['item3'],
                   user[i]['stats']['item4'], user[i]['stats']['item5'], user[i]['stats']['item6']] for i in range(10)]

    total_kills_list = [sum([user[i]['stats']['kills'] for i in range(5)]), sum([user[i]['stats']['kills'] for i in range(5,10)])]
    total_deaths_list = [sum([user[i]['stats']['deaths'] for i in range(5)]), sum([user[i]['stats']['deaths'] for i in range(5,10)])]


    p_to_u = position_check(spells_list, items_list, time_dict)
    if p_to_u == -1:
        return -1

    f_to_u = [time_dict[0]['participantFrames'][str(i+1)]['participantId']-1 for i in range(10)]
    u_to_f = [str(f_to_u.index(i)+1) for i in range(10)]
    p_to_f = [u_to_f[p_to_u[i]] for i in range(10)]


    kda = [[0, 0, 0] for _ in range(10)]
    for i in range(1, 15):
        event_dict = time_dict[i]['events']
        for event in event_dict:
            if event['type'] == 'CHAMPION_KILL':
                kda[event['killerId']-1][0] += 1
                kda[event['victimId']-1][1] += 1
                for assist in event['assistingParticipantIds']:
                    kda[assist-1][2] += 1


    front_list = [[{'champion' : picks_list[p_to_u[i*5+j]],
                    'duration' : duration} for i in range(2)] for j in range(5)]

    result_list = [[{'contribution' :(user[p_to_u[i*5+j]]['stats']['kills']+user[p_to_u[i*5+j]]['stats']['assists']) * 1.0 / max(total_kills_list[i], 1),
                     'death_ratio'  : user[p_to_u[i*5+j]]['stats']['deaths'] * 1.0 / max(total_deaths_list[i], 1),
                     'cs_pm'        : user[p_to_u[i*5+j]]['stats']['totalMinionsKilled'] * 60.0 / duration,
                     'gold_pm'      : user[p_to_u[i*5+j]]['stats']['goldEarned'] * 60.0 / duration} for i in range(2)] for j in range(5)]

    detail_list = [[{'dmg_dealt_champ_pm'  : user[p_to_u[i*5+j]]['stats']['totalDamageDealtToChampions'] * 60.0 / duration,
                     'dmg_dealt_object_pm' : user[p_to_u[i*5+j]]['stats']['damageDealtToObjectives'] * 60.0 / duration,
                     'dmg_dealt_tower_pm'  : user[p_to_u[i*5+j]]['stats']['damageDealtToTurrets'] * 60.0 / duration,
                     'dmg_taken_pm'        : user[p_to_u[i*5+j]]['stats']['totalDamageTaken'] * 60.0 / duration,
                     'dmg_mitigated_pm'    : user[p_to_u[i*5+j]]['stats']['damageSelfMitigated'] * 60.0 / duration,
                     'total_heal_pm'       : user[p_to_u[i*5+j]]['stats']['totalHeal'] * 60.0 / duration,
                     'cs_minion_pm'        : user[p_to_u[i*5+j]]['stats']['totalMinionsKilled'] * 60.0 / duration,
                     'cs_jungle_pm'        : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilled'] * 60.0 / duration,
                     'cs_jungle_team_pm'   : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilledTeamJungle'] * 60.0 / duration,
                     'cs_jungle_enemy_pm'  : user[p_to_u[i*5+j]]['stats']['neutralMinionsKilledEnemyJungle'] * 60.0 / duration,
                     'vision_score_pm'     : user[p_to_u[i*5+j]]['stats']['visionScore'] * 60.0 / duration,
                     'ward_bought_pm'      : user[p_to_u[i*5+j]]['stats']['visionWardsBoughtInGame'] * 60.0 / duration,
                     'ward_placed_pm'      : user[p_to_u[i*5+j]]['stats']['wardsPlaced'] * 60.0 / duration,
                     'ward_killed_pm'      : user[p_to_u[i*5+j]]['stats']['wardsKilled'] * 60.0 / duration} for i in range(2)] for j in range(5)]

    laning_list = [[{'level_14min'   : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['level'],
                     'xp_14min'      : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['xp'],
                     'gold_14min'    : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['totalGold'],
                     'cs_14min'      : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['minionsKilled'] + time_dict[14]['participantFrames'][p_to_f[i*5+j]]['jungleMinionsKilled'],
                     'kills_14min'   : kda[p_to_u[i*5+j]][0],
                     'deaths_14min'  : kda[p_to_u[i*5+j]][1],
                     'assists_14min' : kda[p_to_u[i*5+j]][2]} for i in range(2)] for j in range(5)]
    """
    laning_list = [[{'level_14min'   : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['level'],
                     'xp_14min'      : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['xp'],
                     'gold_14min'    : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['totalGold'],
                     'cs_minion_14min' : time_dict[14]['participantFrames'][p_to_f[i*5+j]]['minionsKilled'],
                     'cs_jungle_14min' :  time_dict[14]['participantFrames'][p_to_f[i*5+j]]['jungleMinionsKilled'],
                     'kills_14min'   : kda[p_to_u[i*5+j]][0],
                     'deaths_14min'  : kda[p_to_u[i*5+j]][1],
                     'assists_14min' : kda[p_to_u[i*5+j]][2]} for i in range(2)] for j in range(5)]
    """
    return front_list, result_list, detail_list, laning_list



def make_data_from_gameid(gameid, key):
  data = requests.get('https://kr.api.riotgames.com/lol/match/v4/matches/'+str(gameid)+'?api_key='+key).json()
  time = requests.get('https://kr.api.riotgames.com/lol/match/v4/timelines/by-match/'+str(gameid)+'?api_key='+key).json()['frames']
  """
  column_list = {'result': ['contribution', 'death_ratio', 'gold_pm', 'cs_pm'],
                 'detail': ['dmg_dealt_champ_pm', 'dmg_dealt_object_pm' ,'dmg_dealt_tower_pm', 'dmg_taken_pm', 'dmg_mitigated_pm', 'total_heal_pm', 'cs_minion_pm', 'cs_jungle_pm',
                            'cs_jungle_team_pm', 'cs_jungle_enemy_pm', 'vision_score_pm', 'ward_bought_pm', 'ward_placed_pm', 'ward_killed_pm'],
                 'laning': ['level_14min', 'xp_14min', 'gold_14min', 'cs_14min', 'kills_14min', 'deaths_14min', 'assists_14min']}
  """
  column_list = {'result': ['contribution', 'death_ratio', 'gold_pm', 'cs_pm'],
                 'detail': ['dmg_dealt_champ_pm', 'dmg_taken_pm', 'vision_score_pm'],
                 'laning': ['xp_14min', 'gold_14min', 'cs_14min', 'kills_14min', 'deaths_14min', 'assists_14min']}


  front, result, detail, laning = get_match(data, time)
  print(front)

  duration_df = pd.read_csv('dataset/s11/v01/top/stat_duration.csv')
  duration_avg = duration_df.iloc[0][0]
  duration_std = duration_df.iloc[1][0]

  duration = front[0][0]['duration']
  interval_time = [0, 1204, 1333, 1464, 1581, 1685, 1788, 1899, 2033, 2223, 8000]
  for i in range(10):
    if interval_time[i+1] > duration:
      interval = i
      break

  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  table_list = ['result', 'detail', 'laning']

  stat = dict(zip(db_list, [dict(zip(table_list, [pd.read_csv('dataset/s11/v01/%s/stat_%s.csv' % (db, table), index_col=0) for table in table_list])) for db in db_list]))

  data_front = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_result_self = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_detail_self = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_laning_self = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_result_diff = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_detail_diff = [[[], []], [[], []], [[], []], [[], []], [[], []]]
  data_laning_diff = [[[], []], [[], []], [[], []], [[], []], [[], []]]


  for i in range(5):
    champ_onehot = pd.read_csv('dataset/s11/v01/%s/champ_dict.csv' % (db_list[i]))
    champ_dict = dict(zip([int(float(champ)) for champ in list(champ_onehot.columns)], list((-1) * champ_onehot.iloc[0])))

    stat_db = stat[db_list[i]]

    for j in range(2):
      champion = front[i][j]['champion']
      if champion not in champ_dict:
        champion = 2000

      champ = [0 for _ in range(len(champ_dict))]
      champ[champ_dict[champion]] = 1

      data_front[i][j] = [1 if index == champ_dict[champion] else 0 for index in range(len(champ_dict))] + [(front[i][j]['duration'] - duration_avg) / duration_std]

      data_result_self[i][j] = [(result[i][j][column] - stat_db['result'][column]['%d_self_mean'%interval]) / stat_db['result'][column]['%d_self_std'%interval] for column in column_list['result']]
      data_detail_self[i][j] = [(detail[i][j][column] - stat_db['detail'][column]['%d_self_mean'%interval]) / stat_db['detail'][column]['%d_self_std'%interval] for column in column_list['detail']]
      data_laning_self[i][j] = [(laning[i][j][column] - stat_db['laning'][column]['%d_self_mean'%interval]) / stat_db['laning'][column]['%d_self_std'%interval] for column in column_list['laning']]

      data_result_diff[i][j] = [(result[i][j][column] - result[i][1-j][column]) / stat_db['result'][column]['%d_diff_std'%interval] for column in column_list['result']]
      data_detail_diff[i][j] = [(detail[i][j][column] - detail[i][1-j][column]) / stat_db['detail'][column]['%d_diff_std'%interval] for column in column_list['detail']]
      data_laning_diff[i][j] = [(laning[i][j][column] - laning[i][1-j][column]) / stat_db['laning'][column]['%d_diff_std'%interval] for column in column_list['laning']]


  return data_front, data_result_self, data_detail_self, data_laning_self, data_result_diff, data_detail_diff, data_laning_diff





def main():
  key = 'RGAPI-59feeb81-5d2c-49b0-b4f1-291b54033929'
  db_list = ['top', 'jgl', 'mid', 'bot', 'sup']
  #gameid_list = [4919026773, 4918920874, 4909199859]
  gameid_list = [4919026773]
  """
  model_list = ['012_3_4', '012_4_3', '013_2_4', '013_4_2', '014_2_3', '014_3_2', '023_1_4', '023_4_1', '024_1_3', '024_3_1', '034_1_2', '034_2_1',
                '123_0_4', '123_4_0', '124_0_3', '124_3_0', '134_0_2', '134_2_0', '234_0_1', '234_1_0']
  method_list = ['sssddd', 'ssdd', 'cssdd', 'csss', 'cddd']
  """
  model_list = ['012_3_4', '012_4_3']
  method_list = ['small']

  percentage = {}
  for method in method_list:
    for db in db_list:
      percentage[method+db] = pd.read_csv('model/%s/%s/percent.csv' % (method, db))


  for gameid in gameid_list:
    front, result_self, detail_self, laning_self, result_diff, detail_diff, laning_diff = make_data_from_gameid(gameid, key)

    X = {}
    #X['csssddd'] = [[front[i][j] + result_self[i][j] + detail_self[i][j] + laning_self[i][j] + result_diff[i][j] + detail_diff[i][j] + laning_diff[i][j] for j in range(2)] for i in range(5)]
    X['small']  = [[result_self[i][j] + detail_self[i][j] + laning_self[i][j] + result_diff[i][j] + detail_diff[i][j] + laning_diff[i][j] for j in range(2)] for i in range(5)]
    #X['csss']    = [[front[i][j] + result_self[i][j] + detail_self[i][j] + laning_self[i][j] for j in range(2)] for i in range(5)]
    X['cddd']    = [[front[i][j] + result_diff[i][j] + detail_diff[i][j] + laning_diff[i][j] for j in range(2)] for i in range(5)]
    X['ddd']     = [[result_diff[i][j] + detail_diff[i][j] + laning_diff[i][j] for j in range(2)] for i in range(5)]
    #X['cssdd']   = [[front[i][j] + result_self[i][j] + detail_self[i][j] + result_diff[i][j] + detail_diff[i][j] for j in range(2)] for i in range(5)]
    #X['ssdd']    = [[result_self[i][j] + detail_self[i][j] + result_diff[i][j] + detail_diff[i][j] for j in range(2)] for i in range(5)]

    """
    for i in range(5):
      model1 = models.load_model('model/%s/zscore012_3_4' % db_list[i])
      model2 = models.load_model('model/%s/zscore012_4_3' % db_list[i])
      model3 = models.load_model('model/test/%s_012_3_4' % db_list[i])
      
      accuracy1 = model1.predict(np.array(X[i]))
      accuracy2 = model2.predict(np.array(X[i]))
      accuracy3 = model3.predict(np.array(X[i]))
      
      print(accuracy1)
      print(accuracy2)
      print(accuracy3)
    """

    for method in method_list:
      for i in range(5):
        print(db_list[i])
        db = db_list[i]

        percent = [0, 0]

        for model_name in model_list:
          model = models.load_model('model/%s/%s/model_%s' % (method, db, model_name))
          predict = model.predict(np.array(X[method][2]))
          print(predict)

          for j in range(2):
            point = predict[j, 1]
            for k in range(1001):
              if percentage[method+db]['model_'+model_name][k] > point:
                percent[j] += k-1
                break

        print(percent[0] / 20, percent[1] / 20)
      print()
    print()

if __name__ == "__main__":
  main()
