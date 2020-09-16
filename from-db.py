# -*- coding:utf-8 -*-

from termios import TCOON
import uuid
import json
import mysql.connector

config = {
}

to_config = {
}

conn = mysql.connector.connect(**config)
to_conn = mysql.connector.connect(**to_config)

# select from 35th t_trend_analysis
def select(query, buffered=True):
  global conn
  cursor = conn.cursor(buffered=buffered)
  cursor.execute(query)
  return cursor

# insert all
def mergeMany(query, values, buffered=True):
  global to_conn

  try:
    cursor = to_conn.cursor(buffered=buffered)
    cursor.executemany(query, values)
    to_conn.commit()
  except Exception as e:
    to_conn.rollback()
    raise e

def makeAnalysisInfoSeq():
  select_query = 'SELECT * FROM T_TREND_ANALYSIS'

  analysis_json = {}

  for row in select(select_query):
    while True:
      uuid_str = str(uuid.uuid4())

      if analysis_json.get(uuid_str) == None:
        analysis_json[uuid_str] = str(row[0])
        break

  analysis_json = dict((v, k) for k, v in analysis_json.items())

  with open('analysis_id_with_random_uuid.json', 'w') as json_file:
    json.dump(analysis_json, json_file)

def makeAnalysisFileSeq():
  select_query = 'SELECT * FROM t_trend_analysis_file'

  file_json_data = {}

  for row in select(select_query):
    while True:
      uuid_str = str(uuid.uuid4())

      if file_json_data.get(uuid_str) == None:
        file_json_data[uuid_str] = str(row[0])
        break

  file_json_data = dict((v, k) for k, v in file_json_data.items())

  with open('analysis_file_id_with_random_uuid.json', 'w') as json_file:
    json.dump(file_json_data, json_file)

def insertAllAnalysis():
  select_query = 'SELECT * FROM T_TREND_ANALYSIS'
  to_analysis_data = []

  with open('user_id_with_random_uuid.json') as user_json, open('analysis_id_with_random_uuid.json') as analysis_json:
    user = json.load(user_json)
    analysis = json.load(analysis_json)

    for row in select(select_query):
      if user.get(str(row[1])) != None:
        start_date = row[6][0:4] + '.' + row[6][4:6] + '.' + row[6][6:]
        end_date = row[7][0:4] + '.' + row[7][4:6] + '.' + row[7][6:]
        insert_data = (analysis[str(row[0])], user[str(row[1])], row[2], ('keyword' if row[3] == 'issue' else row[3]), row[4], row[5], start_date, end_date, row[8], row[13], row[9], row[10], row[11], row[12])
        to_analysis_data.append(insert_data)

  insert_query = """
     INSERT INTO t_trend_analysis_info 
     (trend_analysis_info_seq, user_seq, menu_type, analysis_type, search_keyword, compare_keyword, analysis_start_date, analysis_end_date, analysis_title, is_mobile, del_yn, register_date, update_date, delete_date) 
     VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
   """

  mergeMany(insert_query, to_analysis_data)

def insertAllAnalysisFile():
  select_query = 'SELECT * FROM t_trend_analysis_file'

  insert_data = []
  url_path = 'https://static.some.co.kr/sometrend/attachments/analysis/'
  real_path = '/app_nfs/sometrend/analysis/'

  with open('analysis_id_with_random_uuid.json') as analysis_json, open('analysis_file_id_with_random_uuid.json') as json_file:
    analysis = json.load(analysis_json)
    file_uid = json.load(json_file)

    for row in select(select_query):
      if analysis.get(str(row[1])) != None: 
        new_url_path = ''
        new_real_path = ''
        old_uuid = row[2].split('/')[-2]

        if '/share/' in row[2]:
          if 'issue' in row[2]:
            new_url_path = url_path + 'keyword/share/' + old_uuid + '/'    
            new_real_path = real_path + 'keyword/share/' + old_uuid + '/'
          elif 'reputation' in row[2]:
            new_url_path = url_path + 'reputation/share/' + old_uuid + '/'
            new_real_path = real_path + 'reputation/share/' + old_uuid + '/'
          else:
            new_url_path = url_path + 'compare/share/' + old_uuid + '/'
            new_real_path = real_path + 'compare/share/' + old_uuid + '/'
        elif '/mypage/' in row[2]:
          if 'issue' in row[2]:
            new_url_path = url_path + 'keyword/mypage/' + old_uuid + '/'
            new_real_path = real_path + 'keyword/mypage/' + old_uuid + '/'
          elif 'reputation' in row[2]:
            new_url_path = url_path + 'reputation/mypage/' + old_uuid + '/'
            new_real_path = real_path + 'reputation/mypage/' + old_uuid + '/'
          else:
            new_url_path = url_path + 'compare/mypage/' + old_uuid + '/'
            new_real_path = real_path + 'compare/mypage/' + old_uuid + '/'
        else:
          if 'issue' in row[2]:
            new_url_path = url_path + 'keyword/somegal/' + old_uuid + '/'
            new_real_path = real_path + 'keyword/somegal/' + old_uuid + '/'
          elif 'reputation' in row[2]:
            new_url_path = url_path + 'reputation/somegal/' + old_uuid + '/'
            new_real_path = real_path + 'reputation/somegal/' + old_uuid + '/'
          else:
            new_url_path = url_path + 'compare/somegal/' + old_uuid + '/'
            new_real_path = real_path + 'compare/somegal/' + old_uuid + '/'

        data = (file_uid[str(row[0])], analysis[str(row[1])], new_url_path, new_real_path, row[4], row[5], row[6], row[7], row[8])
        insert_data.append(data)

  insert_query = """INSERT INTO t_trend_analysis_file 
    (trend_analysis_file_seq, trend_analysis_info_seq, file_url_path, file_real_path, file_name, del_yn, register_date, update_date, delete_date) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
  """

  mergeMany(insert_query, insert_data)

try:
  # makeAnalysisInfoSeq()
  # makeAnalysisFileSeq()
  # insertAllAnalysis()
  insertAllAnalysisFile()
except Exception as e:
  print(e)
finally:
  conn.close()
  to_conn.close()
