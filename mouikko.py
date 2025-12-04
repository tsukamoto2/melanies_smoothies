# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

import requests # requestsのインポート文

# Write directly to the app
st.title(":cup_with_straw: カスタムスムージーを作成しましょう！ :cup_with_straw: ")
st.write(
    """お好みのフルーツを最大5つまで選んでください！
    """)

name_on_order = st.text_input('スムージーの名前:')
st.write('スムージーの名前は:', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()

# 【修正箇所】テーブル参照を安全な形式に修正 (全て大文字と想定)
# データベース名、スキーマ名、テーブル名を正確に二重引用符で囲み、参照問題を回避します。
my_dataframe = session.table('"SMOOTHIES"."PUBLIC"."FRUIT_OPTIONS"').select(col('FRUIT_NAME'),col('SEARCH_ON'))
# Note: 'SEARCH_ON'列も大文字で指定。もし小文字で作成されている場合は 'search_on' に修正が必要です。

# 選択肢の確認（アプリが正常に動作し始めたらコメントアウトを推奨）
#st.dataframe(data=my_dataframe, use_container_width=True)
#st.stop() # ここで一時停止しているため、データ取得（my_dataframe）に成功すれば、この画面で停止します。

# Convert the Snowpark Dataframe to a Pandas Dataframe so we can use the LOC function
pd_df=my_dataframe.to_pandas()
#st.dataframe(pd_df)
#st.stop()


ingredients_list = st.multiselect(
    '材料を最大5つまで選択してください:'
    , my_dataframe
    )

if ingredients_list:
    ingredients_string = ''
    
    # 選択された各フルーツについて処理
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen +' '

        search_on=pd_df.loc [pd_df ['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')
        
        # 選択されたフルーツの栄養情報を表示
        st.subheader(fruit_chosen + 'の栄養情報')
        
        # API呼び出し（各フルーツに対応するように変更）
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" +fruit_chosen)
        
        # API応答（JSON形式）をDataFrameとしてStreamlitに表示
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    # 注文挿入ステートメント
    my_insert_stmt = """insert into smoothies.public.orders (ingredients,name_on_order)
        values ('""" + ingredients_string +"""', '""" +name_on_order+ """')"""
    
    time_to_insert = st.button('注文を確定')
    
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        
        st.success('スムージーが注文されました！', icon="✅")
