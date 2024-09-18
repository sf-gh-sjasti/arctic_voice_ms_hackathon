import streamlit as st
from snowflake.snowpark import Session
import json
from datetime import datetime
from io import StringIO
import pandas as pd
import os
from PIL import Image

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

secrets = st.secrets["snowflake"]
session = Session.builder.configs(secrets).create()

if st.button("Back"):
  st.session_state.send_email = False
st.subheader("Contact Tasty Bytes")
STAGE_NAME = 'MS_HACKATHON.APP.IMAGES'

sender = st.text_input('Sender', '')
subject = st.text_input('Subject', '')
body = st.text_area("**Body**", 
              value="", 
              placeholder="",
              height=150)

def upload_file_to_stage(file, stage_name, timestamp):
    try:
        base_name, extension = (file.name).rsplit('.', 1)
        new_file_name = f"{base_name}{timestamp}.{extension}"
        file_path = os.path.join(os.getcwd(), new_file_name)
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())

        session.sql(f"PUT file://{file_path} @{stage_name} auto_compress = false overwrite = true;").collect()
        st.success(f"File {file.name} uploaded successfully!")
    except Exception as e:
        st.error(f"Error uploading file: {str(e)}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

cur_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
uploaded_file = st.file_uploader("Choose a file to upload", type=['png'])
if uploaded_file is not None:
    upload_file_to_stage(uploaded_file, STAGE_NAME, cur_timestamp)
email_content = {
  "body": body if body is not None else "Unknown Body",
  "sender": sender if sender is not None else "Unknown Sender",
  "subject": subject if subject is not None else "Unknown Subject",
  "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
}

email_content_str = json.dumps(email_content)
if st.button("**:blue[Send]**"):
  try :
    if uploaded_file is not None:
      base_name, extension = (uploaded_file.name).rsplit('.', 1)
      new_file_name = f"{base_name}{cur_timestamp}.{extension}"
      insert_email = session.sql("CALL v1.INSERT_NEW_EMAIL_APP(?, ?)", [email_content_str, new_file_name]).collect()
    else:
      insert_email = session.sql("CALL v1.INSERT_NEW_EMAIL_APP(?, ?)", [email_content_str, ""]).collect()
    st.success(insert_email[0]['INSERT_NEW_EMAIL_APP'], icon="✅")
    st.info('Processing the Email', icon="ℹ️")
    if uploaded_file is not None:
      file_data = session.sql(f"""select email_id, GET_PRESIGNED_URL(@{STAGE_NAME}, '{new_file_name}', 604800) from v1.email_status_app where IMAGE_FILE_NAME = '{new_file_name}'""").collect()
      # prompt = f"""You are a customer support agent. Look at the image and recommend a resolution based on the issue reported by the user in the {body if body is not None else 'Unknown Body'}. If the concern by the user does not match with the photo respond back with a message asking for further clarification."""
      prompt = f"""You are a customer support agent. Look at the image and recommend a resolution based on the issue reported by the user in the {body if body is not None else 'Unknown Body'}. If the concern by the user does not match with the photo respond back with a message asking for further clarification."""
      # prompt = f"""Describe Image"""
      
      result = session.sql(f"""SELECT V1.OPENAI_IMAGE('{prompt}','{file_data[0][1]}')""").collect()
      result_cleaned = result[0][0].replace('"', '').replace("'", "")
      response_dict = {
                              "responder": "auto",
                              "subject": subject,
                              "body": result_cleaned
                          }
      response_json = json.dumps(response_dict)
      final_response_dict = {
                                "Response": response_json
                            }
      final_response_json = json.dumps(final_response_dict)
      final_response_json_escaped = final_response_json.replace('"', '\\"')
      final_response_json_escaped = final_response_json_escaped.replace("'", "''")

      email_id = file_data[0][0]
      sql_statement = f"""
                          INSERT INTO v1.email_response_app (email_id, sent_ts, email_response) 
                          SELECT {email_id}, CURRENT_TIMESTAMP(), PARSE_JSON('{final_response_json_escaped}')
                          FROM v1.email_status_app
                          WHERE email_id = {email_id};
                      """
      session.sql(sql_statement).collect()
      session.sql(f"""UPDATE v1.email_status_app
        SET 
            email_status_code = 2,
            email_status_def = 'auto',
            last_updated_ts = CURRENT_TIMESTAMP(),
            responded_flag = TRUE
        WHERE email_id = {email_id};""").collect()
      st.success("Email Processed Successfully and Auto Responded", icon="✅")
    
    else:
      process_email = session.sql("CALL v1.PROCESS_AUTO_RESPONSES_APP()").collect()
      st.success(process_email[0]['PROCESS_AUTO_RESPONSES_APP'], icon="✅")
  except Exception as e:
    st.exception(e)