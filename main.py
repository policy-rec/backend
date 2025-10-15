from fastapi import FastAPI, UploadFile, File, Form, HTTPException
# from apscheduler.schedulers.background import BackgroundScheduler
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from document_handling import Document
from pydantic import BaseModel
from database import DBHandler
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional
from logger import Logger
from llm import LLM
import mimetypes
import os
import json
from blob import Blob

load_dotenv()

app = FastAPI(
    title="REC Policy API",
    description="API for REC Policy backend. Test endpoints here.",
    version="1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000",
                  "https://policy-rag-delta.vercel.app"],  # or ["*"] to allow all
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = DBHandler()
llm = LLM()
doc = Document()
log = Logger()
blob = Blob()

DOC_FOLDER = os.environ.get("DOCUMENT_FOLDER", "documents")
CHAT_IMG_FOLDER = os.environ.get("CHAT_IMG_FOLDER", "chat_images")
IMAGE_FOLDER = os.environ.get("IMAGE_FOLDER", "images")
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")
LOG_FILE = os.environ.get("LOG_FILE", "")
os.makedirs(DOC_FOLDER, exist_ok=True)
os.makedirs(CHAT_IMG_FOLDER, exist_ok=True)
os.makedirs(IMAGE_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {".pdf", ".png"}

# def run_message_insertion():
#     log.log_event("SYSTEM", f"Maintenance task ran at {datetime.now()}")
#     if db.__aquire_lock__():
#         while not db.__is_json_empty__():
#             message = db.__read_write_Q__()
#             db.insert_msg(user_id=message[0], chat_id=message[1], sender=message[2], message=message[3])
#             db.__delete_from_Q__()
#         db.__release_lock__()
#     log.log_event("SYSTEM", "Message Queue Emptied")

# scheduler = BackgroundScheduler()
# scheduler.add_job(run_message_insertion, 'cron', hour=5, minute=12)  # Runs every day at 2:00 AM
# scheduler.start()

# Root URL
@app.get("/")
def read_root():
    return {"message": "API is running!"}

@app.post("/upload-document")
async def upload_document(file: UploadFile = File(...)):
    log.log_event("SYSTEM", "[MAIN] /upload-document API Called")
    log.log_event("SYSTEM", "[MAIN] Starting document processing...")
    
    file_location = os.path.join(DOC_FOLDER, str(file.filename))
    with open(file_location, "wb") as f:
        content = await file.read()
        f.write(content)
    log.log_event("SYSTEM", "[MAIN] Document saved to local folder...")

    blob.upload_file(service=blob.authenticate(), file_path=file_location, folder_name="Documents")
    log.log_event("SYSTEM", "[MAIN] Document saved to blob...")

    document_summary = doc.create_document_summary(llm=llm, document_path=file_location)
    log.log_event("SYSTEM", "[MAIN] Document summary created...")

    db.insert_document(path=file_location, description=document_summary, vectorized=True)
    log.log_event("SYSTEM", "[MAIN] Document inserted in Database...")

    doc.upsert_document(document_path=file_location)
    log.log_event("SYSTEM", "[MAIN] Document inserted in PineconeDB...")

    document_images = doc.extract_images_with_context(document_path=file_location)
    log.log_event("SYSTEM", "[MAIN] Images extracted from the document...")

    if document_images:
        # print('why isnt this working', document_images)
        doc.embed_and_upsert_images(llm=llm, db=db, images=document_images)
        log.log_event("SYSTEM", "[MAIN] Images inserted in PineconeDB...")
    
    log.log_event("SYSTEM", "[MAIN] /upload-document API Returned")
    return {
        "status": "200 OK",
        "message": "File saved successfully", 
        "filename": file.filename
    }

@app.post("/chat")
# async def chat_endpoint(userID: int = Form(...), chatID: int = Form(...), text: str = Form(...), image: Optional[UploadFile] = File(None)):
async def chat_endpoint(userID: int = Form(...), chatID: int = Form(...), text: str = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /chat API Called")
    
    response = ""
    image_location = None
    rag_img_ans = None

    # if image:
    #     log.log_event("SYSTEM", f"[MAIN] User {userID} provided an image")
    #     image_location = os.path.join(CHAT_IMG_FOLDER, str(image.filename))
    #     blob.upload_file(service=blob.authenticate(), file_path=image_location, folder_name="ChatImages")
    #     with open(image_location, "wb") as f:
    #         content = await image.read()
    #         f.write(content)

    db.add_message(chat_id=chatID, sender='user', message=text)
    # log.log_event("USER", msg=text, uid=userID, cid=1)
    # if db.__aquire_lock__():
    #     db.__release_lock__()
    # else:
    #     db.__insert_Write_Q__(user_id=userID, chat_id=1, sender='user', msg=text)

    user_conversation, _ = db.get_chat_msgs(chat_id=chatID)
    input_classification = llm.validate(user_input=text, document_context=db.get_all_doc_descriptions(), user_convo=user_conversation)
    log.log_event("RESP", msg=f"[MAIN] Validator: {input_classification}", uid=userID, cid=1)

    if input_classification == "Valid RAG Question":
        rag_ans = doc.query_text(user_query=text)
        # rag_img_ans = doc.query_images_with_text(query=text)      
        rag_img_ans = None      
        # if rag_img_ans:
            # response = llm.respond(user_input=llm.__format_RLLM_input__(document_context=db.get_all_doc_descriptions(), user_input=text, vllm_classification=input_classification, rag_ans=rag_ans, convo=user_conversation), user_image_path=image_location, rag_image_path=os.path.join(IMAGE_FOLDER, rag_img_ans))
        # else:
        response = llm.respond(user_input=llm.__format_RLLM_input__(document_context=db.get_all_doc_descriptions(), user_input=text, vllm_classification=input_classification, rag_ans=rag_ans, convo=user_conversation), user_image_path='', rag_image_path='')
        log.log_event("RESP", msg=response, uid=userID, cid=1)
    else:
        response = llm.respond(user_input=llm.__format_RLLM_input__(document_context=db.get_all_doc_descriptions(), user_input=text, vllm_classification=input_classification, rag_ans=None, convo=user_conversation), user_image_path=None, rag_image_path=None)
        log.log_event("RESP", msg=response, uid=userID, cid=1)

    if response:
        if rag_img_ans:
            db.add_message(chat_id=chatID, sender='bot', message=response + '\n\nimage:' + os.path.join(IMAGE_FOLDER, rag_img_ans))
        else:
            db.add_message(chat_id=chatID, sender='bot', message=response)
    else:
        db.add_message(chat_id=chatID, sender='bot', message='Oops! Something went wrong.')
    # if db.__aquire_lock__():
    #     db.__release_lock__()
    # else:
    #     db.__insert_Write_Q__(user_id=userID, chat_id=1, sender='bot', msg=response)

    log.log_event("SYSTEM", "[MAIN] /chat API Returned")
    return {
        "status": "200 OK",
        "userID": userID,
        "text": text,
        "class": input_classification,
        "response": response,
        "image_answer": [rag_img_ans] if rag_img_ans else None
    }

class ChatMessage(BaseModel):
    message_id: int
    sender: str
    content: str
    timestamp: datetime

@app.get("/getuserchats")
def get_user_chats(user_id: int):
    log.log_event("SYSTEM", f"[MAIN] /getuserchats/{user_id} API Called")
    raw_chat_data = db.get_chats(user_id=user_id)
    log.log_event("SYSTEM", f"[MAIN] /getuserchats/{user_id} API Returned")
    return raw_chat_data

@app.get("/getchatmessages")
async def get_chat(chat_id: int):
    log.log_event("SYSTEM", f"[MAIN] /getchatmessage/{chat_id} API Called")
    
    _, chatmsgs = db.get_chat_msgs(chat_id=chat_id)

    log.log_event("SYSTEM", f"[MAIN] /getchatmessage/{chat_id} API Returned")
    return chatmsgs

@app.get("/get-image")
async def get_file(filename: str, inline: bool = False):
    log.log_event("SYSTEM", "[MAIN] /get-image API Called")
    if ".." in filename or filename.startswith("/"):
        raise HTTPException(status_code=400, detail="Invalid filename")

    file_path = os.path.join(IMAGE_FOLDER, filename)

    if not os.path.exists(file_path):
        log.log_event("SYSTEM", "[MAIN] Requested image does not exists")
        raise HTTPException(status_code=404, detail="File not found")

    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        log.log_event("SYSTEM", "[MAIN] Incorrect file type")
        raise HTTPException(status_code=403, detail="File type not allowed")

    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type is None:
        mime_type = "application/octet-stream"

    log.log_event("SYSTEM", "[MAIN] /get-image API Returned")
    return FileResponse(
        path=file_path,
        media_type=mime_type,
        filename=filename,
        headers={"Content-Disposition": f"{'inline' if inline else 'attachment'}; filename={filename}"}
    )

@app.post("/authenticate")
async def authenticate_endpoint(username: str = Form(...), password: str = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /authenticate_endpoint called")
    try:
        user: dict | None  = db.authenticate_user(username=username, password=password)
        
        if user:
            log.log_event("SYSTEM", "[MAIN] /authenticate_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "User authentication successful",
                    "userID": user.get("user_id"),
                    "role": user.get("role"),
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /authenticate_endpoint returned - status(401)")
            return JSONResponse(
                status_code=401,
                content={
                    "message": "User ID or password incorrect",
                    "username": username,
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /authenticate_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to authenticate",
                "exception": excp
            }
        )

@app.post("/deactivate-user")
async def deactivate_user_endpoint(userID: int = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /deactivate_user_endpoint called")
    try:
        user = db.deactivate_user(user_id=userID)

        if user:
            log.log_event("SYSTEM", "[MAIN] /deactivate_user_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "User deactivated successfully",
                    "userID": userID,
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /deactivate_user_endpoint returned - status(404)")
            return JSONResponse(
                status_code=404,
                content={
                    "message": "User not found",
                    "userID": userID
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /deactivate_user_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": f"Failed to deactivate user: {userID}",
                "exception": excp
            }
        )
    
@app.post("/activate-user")
async def activate_user_endpoint(userID: int = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /activate_user_endpoint called")
    try:
        user = db.activate_user(user_id=userID)

        if user:
            log.log_event("SYSTEM", "[MAIN] /activate_user_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "User activated successfully",
                    "userID": userID,
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /activate_user_endpoint returned - status(404)")
            return JSONResponse(
                status_code=404,
                content={
                    "message": "User not found",
                    "userID": userID
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /activate_user_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": f"Failed to activate user: {userID}",
                "exception": excp
            }
        )
    
@app.get("/refresh-logs")
async def refresh_logs_endpoint():
    log.log_event("SYSTEM", "[MAIN] /refresh_logs_endpoint called")
    try:
        log_file = blob.upload_file(blob.authenticate(), file_path=os.path.join(LOG_FOLDER, LOG_FILE), folder_name="Logs")

        if log_file:
            log.log_event("SYSTEM", "[MAIN] /refresh_logs_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Logs have been refreshed",
                    "file_name": log_file.get("name"),
                    "file_id": log_file.get("id"),
                    "file_link": log_file.get("webViewLink")
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /refresh_logs_endpoint returned - status(500)")
            return JSONResponse(
                status_code=500,
                content={
                    "message": "Failed to refresh logs"
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /refresh_logs_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to refresh logs",
                "exception": excp
            }
        )
    
@app.get("/get-user-info")
async def get_user_info_endpoint(userID: int):
    log.log_event("SYSTEM", "[MAIN] /get_user_info_endpoint called")

    try:
        user = db.get_user_info(user_id=userID)

        if user:
            enc_user = jsonable_encoder(user)
            log.log_event("SYSTEM", "[MAIN] /get_user_info_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "user": enc_user
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={
                    "message": "User not found",
                    "userID": userID
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /get_user_info_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to fetch user information",
                "userID": userID
            }
        )
    
@app.get("/get-all-users")
async def get_all_users_endpoint():
    log.log_event("SYSTEM", "[MAIN] /get_all_users_endpoint called")
    try:
        user = db.get_all_users_info()

        if user:
            print('1')
            enc_user = jsonable_encoder(user)
            log.log_event("SYSTEM", "[MAIN] /get_all_users_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "users": enc_user,
                    "total_users": len(enc_user),
                    "total_chats": sum(u.get("no_of_chats", 0) for u in enc_user)
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={
                    "message": "Users not found",
                }
            )
    except Exception as excp:
        print('2')
        log.log_event("SYSTEM", "[MAIN] /get_all_users_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to fetch users information",
            }
        )
    
@app.post("/create-user")
async def create_user_endpoint(username: str = Form(...), password: str = Form(...), role: str = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /create_user_endpoint called")
    try:
        user = db.create_user(username=username, password=password, role=role)

        if user:
            log.log_event("SYSTEM", "[MAIN] /create_user_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"User: {username} created successfully.",
                    "user": user
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /create_user_endpoint returned - status(500)")
            return JSONResponse(
                status_code=500,
                content={
                    "message": "User creation failed."
                }
            )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /create_user_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "User creation failed.",
                "exception": excp
            }
        )

@app.post("/create-chat")
async def create_chat_endpoint(userID: int = Form(...), chat_name: Optional[str] = Form(...)):
    try:
        log.log_event("SYSTEM", "[MAIN] /create_chat_endpoint called")
        if chat_name:
            chat = db.create_chat(user_id=userID, chat_name=chat_name)
        else:
            chat = db.create_chat(user_id=userID)

        if chat:
            log.log_event("SYSTEM", "[MAIN] /create_chat_endpoint returned - status(200)")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Chat created successfully.",
                    "chat": chat
                }
            )
        else:
            log.log_event("SYSTEM", "[MAIN] /create_chat_endpoint returned - status(500)")
            return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to create chat.",
            }
        )
    except Exception as excp:
        log.log_event("SYSTEM", "[MAIN] /create_chat_endpoint returned - status(500)")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Failed to create chat.",
                "exception": excp
            }
        )

@app.post("/change-user-details")
async def change_user_details_endpoint(userID: int = Form(...), role: str | None = Form(...), password: str | None = Form(...)):
    log.log_event("SYSTEM", "[MAIN] /change_user_details_endpoint called")
    user_role = None
    user_password = None
    
    try:
        if role:
            user_role = db.change_role(user_id=userID, role=role)

        if password:
            user_password = db.change_password(user_id=userID, password=password)

        if user_role or user_password:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"User details changed successfully.",
                }
            )
        else:
            return JSONResponse(
                status_code=500,
                content={
                    "message": "Incorrect userID or role."
                }
            )
    except Exception as excp:
        return JSONResponse(
            status_code=500,
            content={
                "message": "User role changed failed.",
                "exception": excp
            }
        )
