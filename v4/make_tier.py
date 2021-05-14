import pandas as pd
from copy import deepcopy

def get_tier(summoner_list):
    tier_to_point = {'IRON':0, 'BRONZE':4, 'SILVER':8, 'GOLD':12, 'PLATINUM':16, 'DIAMOND':20, 'MASTER':23, 'GRANDMASTER':25, 'CHALLENGER':27}
    rank_to_point = {'IV':0, 'III':1, 'II':2, 'I':3}
    point_to_tier_under_m = {0:'IRON', 1:'BRONZE', 2:'SILVER', 3:'GOLD', 4:'PLATINUM', 5:'DIAMOND'}
    point_to_rank = {0:' IV', 1:' III', 2:' II', 3:' I'}
    point_to_tier_over_m = {0:'DIAMOND I', 1:'MASTER', 2:'GRANDMASTER', 3:'CHALLENGER'}
    
    point_sum = 0
    ranked_count = 0
    for k in range(10):
        summoner_df = summoner_list[k]

        if len(summoner_df) == 0: # unranked
            continue
        if 'RANKED_SOLO_5x5' not in list(summoner_df['queueType']): # solo unranked
            continue

        summoner_rank_df = summoner_df[summoner_df['queueType']=='RANKED_SOLO_5x5']
        tier = summoner_rank_df['tier'].item()
        rank = summoner_rank_df['rank'].item()

        point = tier_to_point[tier] + rank_to_point[rank]
        ranked_count += 1
        if tier == 'DIAMOND' and rank == 'I':
            point += 1
        point_sum += point

    if ranked_count == 0:
        tier = 'UNRANKED'
    else:
        point_average = point_sum/ranked_count
        if point_average//4 < 6:
            point_under_m = round(point_average) // 4
            if point_under_m == 6:
                tier = 'DIAMOND I'
            else:
                rank_under_m = round(point_average) % 4
                tier = point_to_tier_under_m[point_under_m] + point_to_rank[rank_under_m]
        else:
            point_over_m = round((point_average-24)/2)
            tier = point_to_tier_over_m[point_over_m]
  
    return tier
