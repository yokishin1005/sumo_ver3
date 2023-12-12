from time import sleep
from bs4 import BeautifulSoup
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe

#変数urlにSUUMOホームページをのURLを格納する
url='https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ra=013&cb=0.0&ct=20.0&et=5&md=05&md=06&md=07&md=08&md=09&md=10&md=11&md=12&md=13&md=14&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2=&rn=0005&ek=012505180&ek=012504030&ek=012506530&ek=012510270&ek=012540650&ek=012520110&ek=012531160&ek=012523500&ek=012584570&ek=012517460&ek=012505480&ek=012508940&rn=0125&rn=0160&rn=0010&rn=0015&rn=0020&rn=0025&rn=0030&rn=0040&page={}'

#リストを作成
property_list = []

#1p～20pまで情報を取得
for i in range(1,20):
    
    print(len(property_list))

    target_url = url.format(i)

    print(target_url)

    r = requests.get(target_url)

    sleep(1)

    soup = BeautifulSoup(r.text)

    #全てのcasseteitem情報を取得
    contents = soup.find_all('div', class_='cassetteitem')

    #各物件情報をforループで取得する
    for content in contents:

        #物件情報を取得しておく
        detail = content.find('div', class_='cassetteitem-detail')

        #物件名と住所を取得
        title = detail.find('div', class_='cassetteitem_content-title').text
        station_near = detail.find('div', class_='cassetteitem_detail-text').text
        address = detail.find('li', class_='cassetteitem_detail-col1').text

        #各部屋情報(階数、家賃、敷金＆礼金、広さ)を取得
        table = content.find('table', class_='cassetteitem_other')
        tr_tags = table.find_all('tr', class_='js-cassette_link')
        floor, price, first_fee, capacity = tr_tags[0].find_all('td')[2:6]

        fee, management_fee = price.find_all('li')
        deposit, gratuity = first_fee.find_all('li')
        madori, menseki = capacity.find_all('li')

        property = {
            '物件名': title,
            '最寄り駅': station_near,
            '住所': address,
            '間取り': madori.text,
            '家賃': fee.text,
            '階': floor.text
        }


        #取得した辞書をd_listに格納する
        property_list.append(property)

#階列から、「階」を削除
df['階'] = df['階'].apply(lambda x: x.replace('階', ''))
#家賃から、「万円」を削除
df['家賃'] = df['家賃'].apply(lambda x: x.replace('万円', ''))
#最寄駅から、駅までの距離だけを抽出し新規列に追加→最寄り駅の後ろに並び替える
df[['最寄り駅', '駅までの距離']] = df['最寄り駅'].str.split(' ', expand=True)

#最寄り駅から、「東京メトロ」を削除
df['最寄り駅'] = df['最寄り駅'].apply(lambda x: x.replace('東京メトロ', ''))

#最寄り駅カラムを路線名、駅名に分ける
df[['路線', '駅']] = df['最寄り駅'].str.split('/', expand=True)
df.drop('最寄り駅', axis=1, inplace=True)

#住所から、「東京都」を削除
df['住所'] = df['住所'].apply(lambda x: x.replace('東京都', ''))

<<<<<<< HEAD:suuumoscrver3/SUUMO_ver3.py
=======
#駅までの距離=駅までの距離（徒歩/分）に変更
df = df.rename(columns={'駅までの距離': '駅までの距離（徒歩/分）'})

#列の順番を変更
new_column_order = ['物件名', '最寄り駅', '駅までの距離（徒歩/分）', '間取り','階','家賃', '住所']

#新しいdfを作成
df = df[new_column_order]

>>>>>>> e5b7f78c22c6a10ff3a953a79284f8436d08d676:SUUMO_ver2.py
#駅までの距離から、歩と分を消す
df['駅までの距離（徒歩/分）'] = df['駅までの距離（徒歩/分）'].apply(lambda x: x.replace('歩', ''))
df['駅までの距離（徒歩/分）'] = df['駅までの距離（徒歩/分）'].apply(lambda x: x.replace('分', ''))

#物件名から、東京メトロ、JR、駅を削除
df['物件名'] = df['物件名'].apply(lambda x: x.replace('東京メトロ','').replace('JR', '').replace('駅', ''))

#順番を変更
new_column_order = ['物件名', '路線','駅',  '駅までの距離（徒歩/分）', '間取り','階','家賃', '住所']

#新しいdfを作成
df = df[new_column_order]

#階カラムにある不要なスペース、改行を削除
df['階'] = df['階'].str.replace(r'\s+', '', regex=True)

# 数値データのみを抽出
numeric_df = df[df['階'].str.isnumeric()]

#間取りを間取りタイプ、と統計用のカラムを加える
df['間取りタイプ'] = df['間取り'].str.extract('([1-9][KLD]*)')

#家賃のデータを数値に変える
df['家賃'] = pd.to_numeric(df['家賃'], errors='coerce')




#住所、家賃、階が一致した物件を重複物件とみなし、重複した行を特定する条件を指定
duplicate_condition = df.duplicated(subset=['住所', '家賃', '階'], keep=False)

# 重複した行を抽出
duplicates = df[duplicate_condition]

# 重複した行を確認
print(duplicates)

# 重複した行を削除
df = df.drop_duplicates(subset=['住所', '家賃', '階'], keep='first')

# dfをcsvに出力
df.to_csv('properties.csv', index=None, encoding='utf-8-sig')

#ライブラリをインポート
import gspread
from oauth2client.service_account import ServiceAccountCredentials

<<<<<<< HEAD:suuumoscrver3/SUUMO_ver3.py


#GoogleDrive,SheetAPIに必要な認証情報を得る
=======
#認証情報を取得
>>>>>>> e5b7f78c22c6a10ff3a953a79284f8436d08d676:SUUMO_ver2.py
scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('tech0scraping-075b6d093eb0.json', scope)
gc = gspread.authorize(credentials)

<<<<<<< HEAD:suuumoscrver3/SUUMO_ver3.py
spreadsheet = gc.open('properties')

# DataFrameのデータをリストに変換
df_values = df.values.tolist()

#既存スプレッドシートに新規シートを作成
spreadsheet.add_worksheet(title="property_list", rows=50, cols=10)

#dfを新規シートに挿入
set_with_dataframe(spreadsheet.worksheet("property_list"), df, include_index=True)

=======
# DataFrameのデータをリストに変換
df_values = df.values.tolist()

# データをスプレッドシートに挿入
spreadsheet.get_worksheet(0).insert_rows(df_values, 2)  
>>>>>>> e5b7f78c22c6a10ff3a953a79284f8436d08d676:SUUMO_ver2.py
