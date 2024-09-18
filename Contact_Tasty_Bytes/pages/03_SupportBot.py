# Import python packages
from datetime import datetime, timedelta, timezone
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete
import snowflake.snowpark.functions as F
from snowflake.core import Root

# Set Streamlit page configuration
st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Tasty Bytes Support: Q&A Assistant")
st.caption(
    f"""Welcome! This application suggests answers to customer questions based 
    on corporate documentation and previous agent responses in support chats.
    """
)

# Get current credentials
session = get_active_session()
root = Root(session)


# Constants
CHAT_MEMORY = 20
CHAT_SERVICE = "SUPPORT_SEARCH_SERVICE"
DATABASE = session.get_current_database()
SCHEMA = 'V1'

background_image_url = session.sql(f"""select GET_PRESIGNED_URL('@MS_HACKATHON.APP.IMAGES', 'QA_background.png', 604800)""").collect()[0][0]

page_bg_css = f"""
<style>

    [data-testid="stAppViewContainer"] {{
        background: none; /* Remove any existing background */
    }}

    [data-testid="stAppViewContainer"] {{
        background-image: url("{background_image_url}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    }}
    
    [data-testid="stChatFloatingInputContainer"] {{
        background-image: url("{background_image_url}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
    }}

    [data-testid="stToolbar"] {{
        right: 2rem;
    }}
</style>
"""

# Inject the custom CSS
st.markdown(page_bg_css, unsafe_allow_html=True)


# Reset chat conversation
def reset_conversation():
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "What question do you need assistance answering?",
        }
    ]


##########################################
#      Select LLM
##########################################
with st.expander("**:gear: Settings**"):
    model = st.selectbox(
        "Change chatbot model:",
        [
            "mistral-large",
            "reka-flash",
            "llama2-70b-chat",
            "gemma-7b",
            "mixtral-8x7b",
            "mistral-7b",
            "snowflake-arctic",
            "open-ai"
        ],
    )


##########################################
#       Cortex Search
##########################################
def get_context(chat):
    chat_summary = summarize(chat)
    return find_similar_doc(chat_summary)


def summarize(chat):
    summary = Complete(
        "mistral-large",
        "Provide the most recent question with essential context from this support chat: "
        + chat,
    )
    return summary.replace("'", "")


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
    st.info("Selected Source: " + results[0]["SOURCE_DESC"])
    return results[0]["INPUT_TEXT"]




##########################################
#       Prompt Construction
##########################################
if "background_info" not in st.session_state:
    st.session_state.background_info = (
        session.table("v1.documents")
        .select("raw_text")
        .filter(F.col("relative_path") == "tasty_bytes_who_we_are.pdf")
        .collect()[0][0]
    )


def get_prompt(chat, context):
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

col1, col2 = st.columns([9,1])
col2.button("**:blue[Reset Chat]**", on_click=reset_conversation)

##########################################
#       Chat with LLM
##########################################
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
            # Get relevant information
            context = get_context(user_message.replace("'", ""))
            # Ask LLM
            prompt = get_prompt(user_message.replace("'", ""), context)
            if model == 'open-ai':
                result = session.sql(f"""SELECT v1.ASK_OPENAI('{prompt}')""").collect()
                response = result[0][0]
            else:
                response = Complete(model, prompt)
            status.update(label="Complete!", state="complete", expanded=False)
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})