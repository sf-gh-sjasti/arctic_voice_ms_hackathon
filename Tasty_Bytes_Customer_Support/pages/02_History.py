import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime
import re

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Email History")

session = get_active_session()

def get_emails_responded_by_agent():
  try:
    agent_history = session.sql("""SELECT 
                                    es.email_id,
                                    es.email_object AS Email_Request,
                                    er.email_response AS Email_Response,
                                    er.sent_ts,
                                    SNOWFLAKE.CORTEX.SENTIMENT(CONCAT('Email from Customer is: ', TO_VARCHAR(es.email_object), ', Response from Company is: ', TO_VARCHAR(er.email_response))) AS sentiment_score
                                    FROM 
                                    v1.email_status_app es
                                    JOIN 
                                    v1.email_response_app er ON es.email_id = er.email_id
                                    WHERE 
                                    1=1
                                    AND es.responded_flag = TRUE
                                    AND es.email_status_code = 3
                                    and er.email_response is not null
                                    ORDER BY 
                                    er.sent_ts DESC;""").to_pandas()
    return agent_history
  except Exception as e:
    st.exception(e)

def get_auto_responded_emails():
  try:
    auto_response_history = session.sql("""SELECT 
                                            es.email_id,
                                            es.email_object AS Email_Request,
                                            er.email_response AS Email_Response,
                                            er.sent_ts,
                                            SNOWFLAKE.CORTEX.SENTIMENT(CONCAT('Email from Customer is: ', TO_VARCHAR(es.email_object), ', Response from Company is: ', TO_VARCHAR(er.email_response))) AS sentiment_score,
                                            es.IMAGE_FILE_NAME as IMAGE_FILE_NAME
                                            FROM 
                                            v1.email_status_app es
                                            JOIN 
                                            v1.email_response_app er ON es.email_id = er.email_id
                                            WHERE 
                                            1=1
                                            AND es.responded_flag = TRUE
                                            AND es.email_status_code = 2
                                            and er.email_response is not null
                                            ORDER BY 
                                            er.sent_ts DESC;""").to_pandas()
    return auto_response_history
  except Exception as e:
    st.exception(e)

agent_emails_count_df = session.sql("""SELECT count(*) as COUNT
                                    FROM v1.email_status_app
                                    WHERE 1=1
                                    AND responded_flag = TRUE
                                    AND email_status_code = 3;""").to_pandas()
auto_emails_count_df = session.sql("""SELECT count(*) as COUNT
                                    FROM v1.email_status_app
                                    WHERE 1=1
                                    AND responded_flag = TRUE
                                    AND email_status_code = 2;""").to_pandas()
agent_emails_count = int(agent_emails_count_df["COUNT"])
auto_emails_count = int(auto_emails_count_df["COUNT"])
total = agent_emails_count + auto_emails_count
col1, col2, col3, col4 = st.columns(4)
col1.markdown(f'''**{str(total)}**  
                  Total Emails''')
col2.markdown(f'''**{str(agent_emails_count)}**  
                  Agent Responded''')
col3.markdown(f'''**{str(auto_emails_count)}**  
                  Auto Responded''')
st.divider()

response_type = st.radio("Response By",
                         ["Agent", "Auto"],
                         horizontal=True)

if response_type == 'Agent':
  emails = get_emails_responded_by_agent()
else:
  emails = get_auto_responded_emails()

for _, row in emails.iterrows():
  email_request = json.loads(row["EMAIL_REQUEST"])
  email_response = json.loads(row["EMAIL_RESPONSE"])
  request_data = json.loads(email_request['Request'].replace('\n', '\\n'))
  response_data = json.loads(email_response['Response'].replace('\n', '\\n'))
  with st.expander(f"{(datetime.strptime(str(row['SENT_TS']), '%Y-%m-%d %H:%M:%S.%f')).strftime('%m/%d/%Y %I:%M%p')} - Subject: {request_data['subject']}"):
    if round(float(row['SENTIMENT_SCORE']), 2) > 0:
      st.success(f"Customer Sentiment: {str(round(float(row['SENTIMENT_SCORE']), 2))}")
    else:
      st.error(f"Customer Sentiment: {str(round(float(row['SENTIMENT_SCORE']), 2))}")
    st.markdown(f"**:black[{response_type} Response:]**")
    st.markdown((datetime.strptime(str(row['SENT_TS']), '%Y-%m-%d %H:%M:%S.%f')).strftime('%m/%d/%Y %I:%M%p'))
    st.markdown(response_data['body'])
    st.markdown("**:black[Email from Customer:]**")
    st.markdown(request_data['sender'])
    st.markdown((datetime.strptime(str(request_data['timestamp']), '%Y-%m-%dT%H:%M:%SZ')).strftime('%m/%d/%Y %I:%M%p'))
    st.markdown(request_data['body'])
    if 'IMAGE_FILE_NAME' in emails.columns and  row['IMAGE_FILE_NAME'] is not None and row['IMAGE_FILE_NAME'] != "":
      url = session.sql(f"select GET_PRESIGNED_URL('@MS_HACKATHON.APP.IMAGES', '{row['IMAGE_FILE_NAME']}', 604800)").collect()[0][0]
      st.image(url)
