from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, DateTime, Boolean, func
from sqlalchemy.orm import relationship, sessionmaker, declarative_base, joinedload
from typing import TypedDict, Tuple, Optional, List, cast
from datetime import datetime, timezone
from dotenv import load_dotenv
from datetime import datetime
import hashlib
import secrets
import base64
import json
import os
from logger import Logger

load_dotenv()
log = Logger()
Base = declarative_base()

class MessageData(TypedDict):
    sender: str
    content: str
    timestamp: datetime

class User(Base):
    __tablename__ = 'users'

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(30), nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_login_date = Column(DateTime)
    is_active = Column(Boolean, default=True)

    chats = relationship("Chat", back_populates="user", cascade="all, delete-orphan")


    def __set_password__(self, password, iterations=100000) -> None:
        salt = secrets.token_bytes(32)

        password_bytes = password.encode('utf-8')
        hash_bytes = hashlib.pbkdf2_hmac('sha256', password_bytes, salt, iterations)

        salt_b64 = base64.b64encode(salt).decode('ascii')
        hash_b64 = base64.b64encode(hash_bytes).decode('ascii')

        self.password_hash = f"{salt_b64}${iterations}${hash_b64}"
    
    def __authenticate_password__(self, password) -> bool:
        try:
            parts = self.password_hash.split('$')
            if len(parts) != 3:
                return False
            
            salt_b64, iterations_str, stored_hash_b64 = parts
            
            salt = base64.b64decode(salt_b64)
            iterations = int(iterations_str)
            
            password_bytes = password.encode('utf-8')
            new_hash_bytes = hashlib.pbkdf2_hmac('sha256', password_bytes, salt, iterations)
            new_hash_b64 = base64.b64encode(new_hash_bytes).decode('ascii')

            return secrets.compare_digest(stored_hash_b64, new_hash_b64)
        except (ValueError, TypeError):
            return False
        
    def __getGMTOffset__(self) -> str:
        offset = datetime.now().astimezone().utcoffset()

        if offset is None:
            return "GMT+00:00"
        
        offset_minutes = offset.total_seconds() / 60

        hours = int(offset_minutes // 60)
        minutes = int(offset_minutes % 60)
        return f"GMT{hours:+02}:{minutes:02}"  

    def __update_login_time__(self) -> None:
        self.last_login_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + self.__getGMTOffset__()

    def __get_chats_metadata__(self) -> list[dict]:
        chats = []

        for chat in self.chats:
            chats.append({
                "chat_id": chat.chat_id,
                "chat_name": chat.title,
                "last_msg": chat.last_msg,
                "timestamp": chat.timestamp,
            })

        return chats
    
    def __get_chat_msgs__(self, formated=True, limit=10, sort=True, by_oldest=True) -> Tuple[Optional[str], Optional[List[MessageData]]]:
        messages: list[MessageData] = []
        formatted_messages = None
        sorted_messages = None

        for chat in self.chats:
            for message in chat.messages:
                messages.append({
                    "sender": message.sender,
                    "content": message.content,
                    "timestamp": message.timestamp,
                })

        if formated:
            messages = messages[-limit:]

            formatted_messages = ""
            for msg in messages:
                sender_label = "[User]" if msg["sender"] == "user" else "[LLM]"
                formatted_messages += f"{sender_label}: {msg["content"]}\n\n"
        
        if sort:
            if by_oldest:
                sorted_messages = sorted(messages, key=lambda x: x["timestamp"])
            else:
                sorted_messages = sorted(messages, key=lambda x: x["timestamp"], reverse=True)

        return formatted_messages, sorted_messages

    def __get_user_info__(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "role": self.role,
            "created_at": self.created_at,
            "last_login": self.last_login_date,
            "is_active": self.is_active,
            "no_of_chats": len(self.chats)
        }    

    def __deactivate_user__(self) -> None:
        self.is_active = False

    def __activate_user__(self) -> None:
        self.is_active = True

    def __change_role__(self, role: str) -> None:
        self.role = role

    @classmethod
    def authenticate(cls, session, username, password) -> dict | None:

        user: User = session.query(cls).filter_by(username=username, is_active=True).first()

        if user and user.__authenticate_password__(password=password):
            user.__update_login_time__()
            session.commit()
            return {
                "user_id": user.user_id,
                "role": user.role,
            }
        return None

    @classmethod
    def create_user(cls, session, username, password, role="user") -> dict:
        user = cls(username=username, role=role)

        user.__set_password__(password=password)

        session.add(user)
        session.commit()
        return {
            "user_id": user.user_id,
        }

    @classmethod
    def get_all_users(cls, session) -> list[dict]:
        users = session.query(cls).options(joinedload(cls.chats)).all()
        all_users_data = [user.__get_user_info__() for user in users]
        return all_users_data
    
class Chat(Base):
    __tablename__ = 'chats'

    chat_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    title = Column(String, nullable=True)
    last_msg = Column(String, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    user = relationship("User", back_populates="chats")
    messages = relationship("ChatMessage", back_populates="chat", cascade="all, delete-orphan")


    def add_message(self, session, sender, message) -> dict:
        new_message = ChatMessage(
            chat_id=self.chat_id,
            sender=sender,
            content=message
        )
        self.last_msg = message

        session.add(new_message)
        session.commit()
        return {
            "message_id": new_message.message_id,
            "content": new_message.content,
        }
    
    def get_chat_messages(self, formated=True, limit=10, sort=True, by_oldest=True) -> Tuple[Optional[str], Optional[List[MessageData]]]:
        messages: list[MessageData] = []
        formatted_messages = None
        sorted_messages = None

        for message in self.messages:
                messages.append({
                    "sender": message.sender,
                    "content": message.content,
                    "timestamp": message.timestamp,
                })

        if formated:
            messages = messages[-limit:]

            formatted_messages = ""
            for msg in messages:
                sender_label = "[User]" if msg["sender"] == "user" else "[LLM]"
                formatted_messages += f"{sender_label}: {msg["content"]}\n\n"
        
        if sort:
            if by_oldest:
                sorted_messages = sorted(messages, key=lambda x: x["timestamp"])
            else:
                sorted_messages = sorted(messages, key=lambda x: x["timestamp"], reverse=True)

        return formatted_messages, sorted_messages
    @classmethod
    def create_chat(cls, session, user_id, chat_name="--Untitled--") -> dict:
        chat = cls(user_id=user_id, title=chat_name)

        session.add(chat)
        session.commit()

        return {
            "chat_id": chat.chat_id,
            "title": chat.title,
        }

class ChatMessage(Base):
    __tablename__ = 'chat_message'

    message_id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, ForeignKey('chats.chat_id'), nullable=False)
    sender = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    chat = relationship("Chat", back_populates="messages")

class Image(Base):
    __tablename__ = 'images'

    image_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    extension = Column(String, nullable=False)
    path = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    document_id = Column(Integer, ForeignKey('documents.document_id'), nullable=False)
    page_no = Column(Integer, nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    document = relationship("Document", back_populates="images")


    def get_image_path(self) -> str:
        return cast(str, self.path)
    
    @classmethod
    def get_all_descriptions(cls, session):
        images = session.query(cls).all()
        return [{image.image_id: image.description} for image in images]
    
class Document(Base):
    __tablename__ = 'documents'

    document_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    extension = Column(String, nullable=False)
    path = Column(String, nullable=False)
    description = Column(String, nullable=True)
    vectorized = Column(Boolean, nullable=False, default=False)
    upload_timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    images = relationship("Image", back_populates="document", cascade="all, delete-orphan")

    def mark_vectorized(self, session) -> None:
        self.vectorized = True
        session.commit()
    
    def get_document_path(self):
        return self.path
    
    def add_image(self, session, name, extension, path, description, page_no) -> dict:
        image = Image(
            name=name,
            extension=extension,
            path=path,
            description=description,
            document_id=self.document_id,
            page_no=page_no
        )

        session.add(image)
        session.commit()
        return {
            "image_id": image.image_id,
        }
    
    @classmethod 
    def insert_document(cls, session, path, description, vectorized=False) -> dict:
        name = os.path.basename(path)
        extension = os.path.splitext(name)[1]

        document = cls(
            name=name,
            extension=extension,
            path=path,
            description=description,
            vectorized=vectorized
        )

        session.add(document)
        session.commit()
        return {
            "document_id": document.document_id,
        }
    
    @classmethod
    def get_all_descriptions(cls, session) -> str:
        docs = session.query(cls.description).all()
        descriptions = [doc[0] for doc in docs]
        return "\n".join([f"Document {i+1}: {item}\n" for i, item in enumerate(descriptions)])

class DBHandler:
    def __init__(self) -> None:
        self.engine, self.Session = self.__connect__()

    def __connect__(self) -> Tuple:
        load_dotenv()
        db_url = os.environ.get("NEON_DB_URL")
        
        try:
            engine = create_engine(str(db_url), pool_size=10, max_overflow=20, pool_pre_ping=True, echo=False)

            Base.metadata.create_all(engine)
            Session = sessionmaker(bind=engine)

            log.log_event("SYSTEM", "[DATABASE] Connection to DB successful")
            return engine, Session
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Connection to DB failed. {excp}")
            return None, None
    
   
    #####################
    # User class handling
    def create_user(self, username, password, role="user") -> dict | None:
        try:
            with self.Session() as session:
                user = User.create_user(session=session, username=username, password=password, role=role)
                log.log_event("SYSTEM", f"[DATABASE] New user created.")
                return user
        except Exception as excp:
                log.log_event("SYSTEM", f"[DATABASE] User creation failed. {excp}")
                return None

    def get_user_info(self, user_id) -> dict | None:
        try:
            with self.Session() as session:
                user: User = session.query(User).get(user_id)

                if user:
                    user_info = user.__get_user_info__()
                else:
                    log.log_event("SYSTEM", f"[DATABASE] User information retrieval failed. User does not exist.")
                    return None
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] User information retrieval failed. {excp}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] User information retrieved.")
        return user_info
    
    def get_all_users_info(self) -> list[dict] | None:
        try:
            with self.Session() as session:
                all_user_info = User.get_all_users(session=session)
                log.log_event("SYSTEM", f"[DATABASE] All information retrieval successful.")
                return all_user_info
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] All information retrieval successful. {excp}")
            return None
        
    def get_chats(self, user_id) -> list[dict] | None:
        try:
            with self.Session() as session:
                user: User = session.query(User).get(user_id)

                if user:
                    user_chats = user.__get_chats_metadata__()
                else:
                    log.log_event("SYSTEM", f"[DATABASE] Chat retrieval failed. User does not exist.")
                    return None
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Chat retrieval failed. {excp}")
            return None

        log.log_event("SYSTEM", f"[DATABASE] Chats retrieved.")
        return user_chats
        
    def get_chat_msgs(self, chat_id, formated=True, limit=10, sort=True, by_oldest=True) -> Tuple[Optional[str], Optional[List[MessageData]]]:
        try:
                with self.Session() as session:
                    # chat: Chat = session.query(Chat).get(chat_id)
                    chat: Chat = session.get(Chat, chat_id)

                    if chat:
                        user_msgs = chat.get_chat_messages(formated=formated, limit=limit, sort=sort, by_oldest=by_oldest)
                    else:
                        log.log_event("SYSTEM", f"[DATABASE] Chat retrieval failed. User does not exist.")
                        return None, None
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Chat messages retrieval failed. {excp}")
            return None, None
        
        log.log_event("SYSTEM", f"[DATABASE] Chat messages retrieved.")
        return user_msgs
        
    def authenticate_user(self, username, password) -> dict | None:
        try:
            with self.Session() as session:
                user = User.authenticate(session, username, password)
        except Exception as excp:
                log.log_event("SYSTEM", f"[DATABASE] User authentication failed. {excp}")
                return None 

        if user:
            log.log_event("SYSTEM", f"[DATABASE] User:{username} authentication successful.")
        else:
            log.log_event("SYSTEM", f"[DATABASE] User:{username} authentication denied.")
        return user
    
    def deactivate_user(self, user_id) -> bool | None:
        try:
            with self.Session() as session:
                user: User | None = session.query(User).get(user_id)
            
                if user:
                    user.__deactivate_user__()
                    session.commit()
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] User deactivation failed. {excp}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] User deactivation successful.")
        return True
    
    def activate_user(self, user_id) -> bool | None:
        try:
            with self.Session() as session:
                user: User | None = session.query(User).get(user_id)
            
                if user:
                    user.__activate_user__()
                    session.commit()
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] User activation failed. {excp}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] User activation successful.")
        return True
    
    def change_role(self, user_id, role) -> bool | None:
        try:
            with self.Session() as session:
                user: User = session.query(User).get(user_id) 

                if user:
                    if role == 'admin' or role == 'user':
                        user.__change_role__(role=role)
                        session.commit()
                    else:
                        return None
                else:
                    return None
        except Exception as excp:
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] User role changed.")
        return True
    
    def change_password(self, user_id, password) -> bool | None:
        try:
            with self.Session() as session:
                user: User = session.query(User).get(user_id) 
                if user:
                    user.__set_password__(password=password)
                    session.commit()
                else:
                    return None
        except Exception as excp:
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] User password changed.")
        return True
    #####################
    # Chat class handling
    def add_message(self, chat_id, sender, message: str) -> dict | None:
        try:
            with self.Session() as session:
                chat: Chat = session.query(Chat).get(chat_id)

                if chat:
                    new_message = chat.add_message(session=session, sender=sender, message=message)
                else:
                    log.log_event("SYSTEM", f"[DATABASE] Could not find chat: {chat_id}")
                    return None       
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Unable to add {message[:10]}... to chat: {chat_id}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] Added {message[:10]}... to chat: {chat_id}")
        return new_message

    def create_chat(self, user_id, chat_name="--Untitled--") -> dict | None:
        try:
            with self.Session() as session:
                chat = Chat.create_chat(session=session, user_id=user_id, chat_name=chat_name)
        except Exception as excp:
                log.log_event("SYSTEM", f"[DATABASE] Chat creation failed. {excp}")
                return None
        
        log.log_event("SYSTEM", f"[DATABASE] New chat created.")
        return chat
       
    #########################
    # Document class handling
    def insert_document(self, path, description, vectorized=False) -> dict | None:
        try:
            with self.Session() as session:
                document = Document.insert_document(session=session, path=path, description=description, vectorized=vectorized)
                log.log_event("SYSTEM", f"[DATABASE] New document inserted.")
                return document
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Document insertion failed. f{excp}")
            return None
        
    def get_all_doc_descriptions(self) -> str | None:
        try:
            with self.Session() as session:
                descriptions = Document.get_all_descriptions(session=session)
                log.log_event("SYSTEM", f"[DATABASE] Retrieved all document descriptions.")
                return descriptions
        except Exception as excp:
                log.log_event("SYSTEM", f"[DATABASE] Retrieval of document descriptions failed. {excp}")
                return None

    ######################
    # Image class handling
    def get_image_path_by_id(self, image_id) -> str | None:
        try:
            with self.Session() as session:
                image: Image = session.query(Image).get(image_id)

                if image:
                    image_path = image.get_image_path()
                else:
                    log.log_event("SYSTEM", f"[DATABASE] Image path retrieval failed. Image does not exist.")
                    return None
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Image path retrieval failed. {excp}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] Image path retrieval successful.")
        return image_path
    
    def insert_image(self, document_id, name, extension, path, description, page_no) -> dict | None:
        try:
            with self.Session() as session:
                doc: Document = session.get(Document, document_id)

                if doc:
                    image = doc.add_image(session=session, name=name, extension=extension, path=path, description=description, page_no=page_no)
                else:
                    log.log_event("SYSTEM", f"[DATABASE] Image insertion failed. Document does not exist.")
                    return None
        except Exception as excp:
            log.log_event("SYSTEM", f"[DATABASE] Image insertion failed. {excp}")
            return None
        
        log.log_event("SYSTEM", f"[DATABASE] Image insertion successful.")
        return image
    

# db = DBHandler()
# print(db.create_chat(1, "testinggggssss"))
# print(db.add_message(3, 'user', 'WASSUP'))
# _, msgs = db.get_chat_msgs(chat_id=3)
# print(msgs)
# print(db.insert_document('./documents/', 'random document', True))
# print(db.insert_image(1, '123.png', '.png', './images/123.png', 'random image', 3))
# # db.create_user('ali.zain', '789')
# # print(db.authenticate_user('ali.zain', '789'))
# # print(db.deactivate_user(3))
# print(db.get_all_users_info())