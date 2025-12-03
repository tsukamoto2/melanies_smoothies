# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

import requests # requestsのインポート文

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw: ")
st.write(
    """Choose the fruits you want in your custom Smoothie!
    """)

name_on_order = st.text_input('Name_on_Smoothies:')
st.write('The name on your Smoothie will be:', name_on_order)

cnx = st.connection("snowflake")
session = cnx.session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
#st.dataframe(data=my_dataframe, use_container_width=True)

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    , my_dataframe
    )

if ingredients_list:
    ingredients_string = ''
    
    # 選択された各フルーツについて処理
    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen +' '
        st.subheader(fruit_chosen + ' Nutrition Information')
        # 課題の指示通り、API呼び出しとDataFrame表示をforループ内に追加
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" +fruit_chosen)
        # API応答（JSON形式）をDataFrameとしてStreamlitに表示
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

    # st.write(ingredients_string)

    my_insert_stmt = """insert into smoothies.public.orders (ingredients,name_on_order)
        values ('""" + ingredients_string +"""', '""" +name_on_order+ """')"""

    #st.write(my_insert_stmt)
    #st.stop()
    
    time_to_insert = st.button('Submit Order')
    
    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        
        st.success('Your Smoothie is ordered!', icon="✅")


# トップレベルのAPI呼び出しは、課題の指示通りにforループ内に移動したため削除
# smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
# sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)
