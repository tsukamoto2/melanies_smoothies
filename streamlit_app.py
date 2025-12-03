# Import python packages
import streamlit as st
from snowflake.snowpark.functions import when_matched, col, when_not_matched 
import pandas as pd

# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw: ")
st.write(
    """Orders that need to be filled.
    """)

session = get_active_session()
# ----------------------------------------------------
# 注文データ取得とフィルタリング
# ----------------------------------------------------

cnx = st.connection("snowflake")
session = cnx.session()

orders_df = session.table("smoothies.public.orders")
pending_orders_df = orders_df.filter(col('"ORDER_FILLED"') == False) 

# 1. 未処理の注文数をカウント
orders_to_fill = pending_orders_df.count() 

# ----------------------------------------------------
# 2. 注文数に基づく条件分岐
# ----------------------------------------------------

if orders_to_fill > 0:
    # 注文がある場合: 表とボタンを表示
    
    st.write("---") # 区切り線
    st.subheader(f"Open Orders: {orders_to_fill} remaining") # 残り件数を表示
    
    # 編集可能なデータエディタを表示
    editable_df = st.data_editor(pending_orders_df, key="pending_orders_editor")
    
    submitted = st.button('Fill Order', key="fill_order_button") 

    # ----------------------------------------------------
    # MERGE (Submit) ロジック
    # ----------------------------------------------------

    if submitted:
        # 1. Streamlitの変更をSnowpark DataFrameに変換
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        
        # 2. MERGE操作を実行し、データベースへの変更を確定
        og_dataset.merge(edited_dataset,
                         # 結合キー
                         (og_dataset['"ORDER_UID"'] == edited_dataset['"ORDER_UID"']),
                         # 変更があった場合にのみ更新
                         [when_matched().update({'ORDER_FILLED': edited_dataset['"ORDER_FILLED"']})]
        ).collect() 
        
        # 3. 処理完了メッセージ
        st.success('✅ Orders processed successfully! Please refresh.', icon="✅")
        st.experimental_rerun() # 画面をリフレッシュして最新のリストを表示
        
else:
    # 注文がない場合: メッセージを完全に表示
    st.success('There are no pending orders right now.', icon="👍")
