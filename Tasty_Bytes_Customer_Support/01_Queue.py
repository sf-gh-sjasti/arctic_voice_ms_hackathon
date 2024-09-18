import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
import json
import pandas as pd
from snowflake.cortex import Complete
from snowflake.core import Root

st.set_page_config(layout="wide", initial_sidebar_state="expanded")

session = get_active_session()
root = Root(session)

CHAT_MEMORY = 20
DOC_TABLE = "v1.vector_store"
CHAT_SERVICE = "SUPPORT_SEARCH_SERVICE"
DATABASE = session.get_current_database()
SCHEMA = 'V1'


if 'show_reply_option' not in st.session_state:
  st.session_state.show_reply_option = False

if 'show_emails' not in st.session_state:
  st.session_state.show_emails = True

if 'clicked_index' not in st.session_state:
  st.session_state.clicked_index = None

if 'generate_email_response' not in st.session_state:
  st.session_state.generate_email_response = True

if 'chatbot_needed' not in st.session_state:
  st.session_state.chatbot_needed = False

if 'show_knowledge_base_option' not in st.session_state:
  st.session_state.show_knowledge_base_option = False

if 'show_knowledge_base_preview' not in st.session_state:
  st.session_state.show_knowledge_base_preview = False

if 'show_success_message' not in st.session_state:
  st.session_state.show_success_message = False

if 'current_record' not in st.session_state:
  st.session_state.current_record = []

# Getting Emails in Queue to be processed by the agent
def get_emails_in_queue():
  try:
    emails_in_queue = session.sql("""SELECT 
                                      email_id,
                                      email_object
                                      FROM v1.email_status_app
                                      WHERE 1=1
                                      AND responded_flag = FALSE
                                      AND email_status_code = 3;""").to_pandas()
    return emails_in_queue
  except Exception as e:
    st.exception(e)

def on_reply_click(key):
  st.session_state.clicked_index = int(key.split('_')[-1])
  st.session_state.current_record = st.session_state.parsed_emails[st.session_state.clicked_index]
  st.session_state.show_emails = False
  st.session_state.show_reply_option = True
  st.session_state.generate_email_response = True
  st.session_state.chatbot_needed = False
  st.session_state.show_knowledge_base_option = False

def get_context(chat):
  chat_summary = summarize(chat)
  return find_similar_doc(chat_summary)

def summarize(chat):
  prompt = f'''Provide the most recent question with essential context from this support chat: {chat.replace("'", "''")}'''
  summary = Complete('mistral-large', prompt)
  return summary

def find_similar_doc(query):
    cortex_search_service = (
        root.databases[DATABASE]
        .schemas[SCHEMA]
        .cortex_search_services[CHAT_SERVICE]
    )
    
    context_documents = cortex_search_service.search(
        query, columns=["CHUNK", "INPUT_TEXT", "SOURCE_DESC"], limit=1
    )
    results = context_documents.results
    return results[0]["INPUT_TEXT"]

def generate_email_response(email):
  prompt = f"""Generate an email response to send to {email['sender']} 
                who is a customer of A Tasty Bytes Food Truck. Do not inlude Subject and use Tasty Bytes Support Team for Best Regards. Provide response to this customer question sent to our support agent 
                at Tasty Bytes Food Truck Company. Use the background information and provided context taken from the 
                most relevant corporate documents or previous support chat logs with other customers. Be concise and only answer 
                the latest question.
                Email Subject from Customer: <Subject> {email['subject']} </Subject>.
                Customer Email Address: <Email_Address> {email['sender']} </Email_Address>.
                Email body is: <EMAIL_BODY> {email['body']} </EMAIL_BODY>.
                Context: <context> {get_context(email['body'])} </context>.
                Background Info: <background_info> {st.session_state.background_info} </background_info>."""
  response = Complete('mistral-large', prompt.replace("'", "''"))
  return response.strip()

def generate_prompt_for_chat(chat, context):
  prompt = f"""Answer this new customer question sent to our support agent
              at Tasty Bytes Food Truck Company. Use the background information
              and provided context taken from the most relevant corporate documents
              or previous support chat logs with other customers.
              Be concise and only answer the latest question.
              The question is in the chat.
              Chat: <chat> {chat} </chat>.
              Context: <context> {context} </context>.
              Background Info: <background_info> {st.session_state.background_info} </background_info>."""
  return prompt.replace("'", "")

def reset_conversation():
  st.session_state.messages = [
    {
      "role": "assistant",
      "content": "What question do you need assistance answering?",
    }
  ]

def on_go_back_click():
  st.session_state.show_reply_option = False
  st.session_state.show_emails = True

if st.session_state.show_emails:
  st.title("Emails Awaiting Response")
  st.session_state.emails_in_queue = get_emails_in_queue()
  timestamps = []
  for email_object in st.session_state.emails_in_queue['EMAIL_OBJECT']:
      data = json.loads(email_object)
      request_data = json.loads(data['Request'].replace('\n', '\\n'))
      if 'timestamp' in request_data:
          timestamps.append(request_data['timestamp'])
      else:
          timestamps.append(None)
  st.session_state.emails_in_queue['timestamp'] = timestamps
  st.session_state.emails_in_queue['timestamp'] = pd.to_datetime(st.session_state.emails_in_queue['timestamp'], errors='coerce')
  st.session_state.emails_in_queue = st.session_state.emails_in_queue.sort_values(by='timestamp').reset_index(drop=True)

  parsed_emails = []
  for _, row in st.session_state.emails_in_queue.iterrows():
      data = json.loads(row["EMAIL_OBJECT"])
      request_data = json.loads(data['Request'].replace('\n', '\\n'))
      parsed_emails.append(request_data)
  st.session_state.parsed_emails = parsed_emails
  st.markdown(f"**:black[Emails in Queue: {len(st.session_state.parsed_emails)}]**")
  st.divider()
  with st.container():
    for index, row in enumerate(st.session_state.parsed_emails):
      timestamp, subject, reply, temp = st.columns([2,6,1,1])
      timestamp.write((datetime.strptime(str(row['timestamp']), '%Y-%m-%dT%H:%M:%SZ')).strftime('%m/%d/%Y %I:%M%p'))
      # sender.write(row['sender'])
      subject.write(f"Subject: {row['subject']}")
      reply.button("**:blue[Reply]**", on_click=on_reply_click, key=f"ReplyButton_{index}", kwargs={"key": f"ReplyButton_{index}"}, use_container_width=True)
      st.divider()

if st.session_state.show_reply_option:
  st.button("**:blue[:arrow_backward: Go Back]**", on_click=on_go_back_click)
  st.session_state.new_reply = True
  st.subheader(f"**:black[Subject: {st.session_state.current_record['subject']}]**")
  st.write("")
  st.markdown(f"From: {st.session_state.current_record['sender']}")
  st.markdown(f"{(datetime.strptime(str(st.session_state.current_record['timestamp']), '%Y-%m-%dT%H:%M:%SZ')).strftime('%m/%d/%Y %I:%M%p')}")
  st.write("")
  st.markdown(f"{st.session_state.current_record['body']}")
  if 'background_info' not in st.session_state:
    st.session_state.background_info = (
      session.table("v1.documents")
      .select("raw_text")
      .filter(F.col("relative_path") == "tasty_bytes_who_we_are.pdf")
      .collect()[0][0]
    )
  if st.session_state.generate_email_response:
    st.session_state.email_response = (generate_email_response(st.session_state.current_record))
    st.session_state.generate_email_response = False
  st.write("")
  st.session_state.email_response = st.text_area("**:black[Suggested Response:]**", 
               value=st.session_state.email_response, 
               placeholder="Type Your Response Here or Get Suggestions From Auto Reply or Chatbot", 
               height=300)
  col1, col2 = st.columns([8,0.5])
  if col2.button("**:blue[Send]**"):
    email_content_final = {
        "responder": "agent",
        "subject": str(st.session_state.current_record['subject']),
        "body": (st.session_state.email_response).replace('"', '')
    }

    # Convert the dictionary to a VARIANT type
    email_content_variant = json.dumps(email_content_final)
    try :
      temp = session.sql("CALL v1.INSERT_RESPONSE_APP(?,?)", [int(st.session_state.emails_in_queue.loc[st.session_state.clicked_index, 'EMAIL_ID']), email_content_variant]).collect()
      st.success(temp[0]['INSERT_RESPONSE_APP'], icon="✅")
      st.session_state.show_knowledge_base_option = True
    except Exception as e:
      st.exception(e)
  
  if st.session_state.show_knowledge_base_option:
    st.markdown("**:black[Add to Knowledge Base?]**")
    col1, col2, col3 = st.columns([1,1,16])
    if col1.button("**:blue[Yes]**"):
      st.session_state.show_knowledge_base_option = False
      st.session_state.show_knowledge_base_preview = True
    if col2.button("**:black[No]**"):
      st.session_state.show_knowledge_base_option = False
  if st.session_state.show_knowledge_base_preview:
    try :
      prompt = f"""Generate a summary to add to the call center knowledge base. If the bot can't answer a customer's 
                question, it should be transferred to an agent. The agent answers the question, and if it's a 
                recurring query, the summary should be stored in the knowledge base. This way, the bot can utilize 
                the knowledge base for future similar inquiries. Create a summary that includes the question and 
                its corresponding answer for storage in the knowledge base. Question is: 
                {st.session_state.current_record['body']} and answer is:
                {st.session_state.email_response}"""
      response = Complete('mistral-large', prompt.replace("'", "''"))
      st.session_state.summary = st.text_area(f"**:black[Knowledge Base Summary]**", value=str(response).strip().replace("'", "''"), height=200)
      if st.button(f"**:blue[Save]**"):
        insert_chunks = session.sql(f"""INSERT INTO v1.chunk_text
                                        SELECT
                                        'EMAIL' AS source,
                                        'Customer support Email history with a Tasty Bytes Agent' AS source_desc,
                                        '{st.session_state.summary}' AS full_text,
                                        LENGTH('{st.session_state.summary}') AS size,
                                        func.*,
                                        CASE WHEN LENGTH('{st.session_state.summary}') > 12000 THEN func.chunk ELSE '{st.session_state.summary}' END AS input_text
                                        FROM TABLE(v1.text_chunker('{st.session_state.summary}')) AS func;""").collect()
        insert_to_vector_store = session.sql(f"""INSERT into v1.vector_store
                                      SELECT
                                      source,
                                      source_desc,
                                      full_text,
                                      size,
                                      chunk,
                                      input_text,
                                      SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', chunk) AS chunk_embedding
                                      FROM v1.chunk_text
                                      where full_text = '{st.session_state.summary}';""").collect()
        st.session_state.show_success_message = True         
    except Exception as e:
      st.exception(e)
  if st.session_state.show_success_message:
    st.success("Updated to Knowledge Base successfully", icon="✅")   
  st.session_state.chatbot_needed = st.checkbox("Chatbot")
  if st.session_state.chatbot_needed:
    if "messages" not in st.session_state:
      reset_conversation()

    if user_message := st.chat_input():
      st.session_state.messages.append({"role": "user", "content": user_message})

    for message in st.session_state.messages:
      with st.chat_message(message["role"]):
        st.markdown(message["content"])

    if st.session_state.messages[-1]["role"] != "assistant":
      chat = str(st.session_state.messages[-CHAT_MEMORY:]).replace("'", "")
      with st.chat_message("assistant"):
        with st.status("Answering..", expanded=True) as status:
          st.write("Finding relevant documents & support chat logs...")
          context = get_context(chat)
          st.write("Using search results to answer your question...")
          prompt = generate_prompt_for_chat(chat, context)
          response = Complete('mistral-large', prompt.replace("'", "''"))
          status.update(label="Complete!", state="complete", expanded=False)
        st.markdown(response)
      st.session_state.messages.append({"role": "assistant", "content": response})


