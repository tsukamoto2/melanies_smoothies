# Import python packages
import streamlit as st
# pandasをインポートするために必要な行を追加します
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

import requests # requestsのインポート文
# pandasのインポート (Snowpark DataframeをPandasに変換するために必要)
import pandas as pd


# Write directly to the app
st.title(":cup_with_straw: カスタムスムージーを作成しましょう！ :cup_with_straw: ")
st.write(
    """お好みのフルーツを最大5つまで選んでください！
    """)

name_on_order = st.text_input('スムージーの名前:')
st.write('スムージーの名前は:', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()

# テーブル参照を安全な形式に修正 (全て大文字と想定)
my_dataframe = session.table('"SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"').select(col('FRUIT_NAME'),col('SEARCH_ON'))

# 選択肢の確認（アプリが正常に動作し始めたらコメントアウトを推奨）
#st.dataframe(data=my_dataframe, use_container_width=True)
#st.stop() 

# Convert the Snowpark Dataframe to a Pandas Dataframe so we can use the LOC function
pd_df=my_dataframe.to_pandas()
#st.dataframe(pd_df)
#st.stop()


ingredients_list = st.multiselect(
    '材料を最大5つまで選択してください:'
    , pd_df['FRUIT_NAME'] # multiselectの選択肢をPandas Dataframeの列に変更
    )

if ingredients_list:
    ingredients_string = ''
    
    # 選択された各フルーツについて処理
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen +' '

        # Pandasのloc関数を使用して、FRUIT_NAMEに一致するSEARCH_ONの値を取得
        # locのインデックス参照を整数（iloc[0]）に限定してエラーを回避
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        
        st.write('検索値は ', fruit_chosen,' の場合、', search_on, 'です。')
        
        # 選択されたフルーツの栄養情報を表示
        st.subheader(fruit_chosen + 'の栄養情報')
        
        # 【修正箇所】API呼び出し: 引用符を正しく二重引用符で閉じる
        smoothiefroot_response = requests.get(f"https://my.smoothiefroot.com/api/fruit/{search_on}")
        
        # API応答（JSON形式）をDataFrameとしてStreamlitに表示
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    # 注文挿入ステートメント
    my_insert_stmt = """insert into smoothies.public.orders (ingredients,name_on_order)
        values ('""" + ingredients_string +"""', '""" +name_on_order+ """')"""
    
    time_to_insert = st.button('注文を確定')
    
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        
        st.success('スムージーが注文されました！', icon="✅")
