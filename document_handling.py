from clip.simple_tokenizer import SimpleTokenizer as ClipTokenizer
from typing import cast, Optional, Any
from typing import List, Dict, Tuple
import torch.nn.functional as F
from dotenv import load_dotenv
from database import DBHandler
from pinecone import Pinecone
from logger import Logger
from pathlib import Path
from torch import Tensor 
from blob import Blob
from PIL import Image
from llm import LLM
import tiktoken
import torch
import fitz
import uuid
import clip
import os
import re

load_dotenv()
blob = Blob()
log = Logger()

class Document:
    def __init__(self) -> None:
        log.log_event("SYSTEM", "Document class Initialized")
        self.pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
        self.index = self.pc.Index(str(os.environ.get("PINECONE_INDEX_NAME")))
        self.index_images = self.pc.Index(str(os.environ.get("PINECONE_IMAGES_INDEX_NAME")))
        log.log_event("SYSTEM", "Connected to PineconeDB")

    def __clean_text__(self, text: str) -> str:
        text = re.sub(r'\s+', ' ', text)  
        return text.strip()    
    
    def __tokenize__(self, text, max_tokens=250, overlap=100) -> list:
        enc = tiktoken.get_encoding("cl100k_base")
        tokens = enc.encode(text=text)

        chunks = []
        start = 0

        while start < len(tokens):
            end = start + max_tokens
            chunk = tokens[start:end]
            decoded_chunk = enc.decode(chunk)
            
            chunks.append(decoded_chunk)
            start += max_tokens - overlap
        
        return chunks

    def __get_page_text_context__(self, page_text: str, total_images: int, img_index: int, context_chars: int = 500) -> Tuple[str, str]:
        if not page_text.strip():
            return "", ""
        
        text_length = len(page_text)
        
        if total_images == 1:
            split_point = text_length // 2
        else:
            split_point = int((img_index + 1) * text_length / (total_images + 1))
        
        text_before = page_text[:split_point]
        text_after = page_text[split_point:]

        if len(text_before) > context_chars:
            text_before = "..." + text_before[-context_chars:]
        
        if len(text_after) > context_chars:
            text_after = text_after[:context_chars] + "..."
        
        return text_before.strip(), text_after.strip()
    
    def __get_text_context_around_image__(self, page, img_rect, page_text: str, context_chars: int = 500) -> Tuple[str, str]:
        text_blocks = page.get_text("dict")
        
        text_before_img = []
        text_after_img = []

        for block in text_blocks["blocks"]:
            if "lines" not in block:
                continue
                
            block_rect = fitz.Rect(block["bbox"])

            if block_rect.y1 < img_rect.y0:
                block_text = ""
                for line in block["lines"]:
                    for span in line["spans"]:
                        block_text += span["text"] + " "
                text_before_img.append(block_text.strip())
                
            elif block_rect.y0 > img_rect.y1:
                block_text = ""
                for line in block["lines"]:
                    for span in line["spans"]:
                        block_text += span["text"] + " "
                text_after_img.append(block_text.strip())

        context_before = " ".join(text_before_img)
        context_after = " ".join(text_after_img)

        if len(context_before) > context_chars:
            context_before = "..." + context_before[-context_chars:]
        
        if len(context_after) > context_chars:
            context_after = context_after[:context_chars] + "..."
        
        return context_before, context_after
    
    def __truncate_for_clip__(self, text: str, max_tokens: int = 75) -> str:
        tokenizer = ClipTokenizer()
        words = text.strip().split()
        
        low, high = 0, len(words)
        best = ""
        
        while low <= high:
            mid = (low + high) // 2
            candidate = " ".join(words[:mid])
            token_count = len(tokenizer.encode(candidate))

            if token_count <= max_tokens:
                best = candidate
                low = mid + 1
            else:
                high = mid - 1

        return best if best else text[:max_tokens]
    
    def __get_clip_embedding__(self, text: str | None = None, image_path: str | None = None, img_weight: float = 0.4, txt_weight: float = 0.6):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model, preprocess = clip.load("ViT-B/32", device=device)
        model.eval()

        image_vec = None
        text_vec = None

        if image_path:
            image = Image.open(image_path)
            image_tensor = preprocess(image)

            image_tensor = cast(Tensor, image_tensor)

            image_input = image_tensor.unsqueeze(0).to(device)

            with torch.no_grad():
                image_vec = model.encode_image(image).squeeze(0)
                image_vec = F.normalize(image_vec, dim=0)

        if text:
            safe_text = self.__truncate_for_clip__(text)
            text_token = clip.tokenize(safe_text).to(device)
            with torch.no_grad():
                text_vec = model.encode_text(text_token).squeeze(0)
                text_vec = F.normalize(text_vec, dim=0)

        if image_vec is not None and text_vec is not None:
            combined = (img_weight * image_vec + txt_weight * text_vec).tolist()
            return combined
        elif image_vec is not None:
            return image_vec.tolist()
        elif text_vec is not None:
            return text_vec.tolist()
        else:
            raise ValueError("Provide at least one of image_path or text.")
        
    def embed_and_upsert_images(self, llm: LLM, db: DBHandler, images: List[Dict], namespace="images"):        
        records = []

        log.log_event("SYSTEM", "Embedding and Upserting Images --- CLIP Embedding Images")
        for image in images:
            image_desc = llm.generate_image_description(f"Context Before: {image["context_before"]}\n\nContext After: {image["context_after"]}", image["image_path"])
            vector = (self.__get_clip_embedding__(text=image_desc, image_path=image["image_path"]))
            
            records.append({
                "_id": f"{namespace}-chunk-{uuid.uuid4().hex}",
                "values": vector,
                "description": image_desc,
                "source": os.path.basename(image["image_path"]),
                "page_no": image["page_number"],
                "image_no": image["image_number"],
            })
            # print(type(chunk))

        log.log_event("SYSTEM", "Embedding and Upserting Images --- Upserting Images to PineconeDB")
        self.index_images.upsert(
            namespace=namespace,
            vectors=[
                (
                    record["_id"],      
                    record["values"], 
                    {
                        "description": record["description"],
                        "source": record["source"],
                        "page_no": record["page_no"],
                        "image_no": record["image_no"]
                    }               
                )
                for record in records
            ]
        )
        log.log_event("SYSTEM", "Embedding and Upserting Images --- Upserting Images to PineconeDB SUCCESSFUL")

    def upsert_document(self, document_path, namespace="documents"):
        document = fitz.open(document_path)

        cleaned_text = ""
        records = []

        for page_num in range(len(document)):
            # page = document[page_num]
            page = cast(Any, document[page_num])
            blocks = page.get_text("blocks") 
            blocks.sort(key=lambda b: (b[1], b[0]))  

            text = " ".join([b[4].strip() for b in blocks if b[4].strip()])
            cleaned_text += self.__clean_text__(text)

        text_chunks = self.__tokenize__(cleaned_text)

        for chunk in text_chunks:
            records.append({
                "_id": f"{namespace}-chunk-{uuid.uuid4().hex}",
                "text": chunk,
                "source": os.path.basename(document_path),
            })

        log.log_event("SYSTEM", "Upsert Document --- Upserting Records to PineconeDB")

        # 🔹 Batch the records in chunks of 96
        batch_size = 96
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            self.index.upsert_records(namespace=namespace, records=batch)

        log.log_event("SYSTEM", "Upsert Document --- Upserting Records to PineconeDB SUCCESSFUL")

    def extract_images_with_context(self, document_path: str, output_dir: str = "./images") -> List[Dict]:
        os.makedirs(output_dir, exist_ok=True)
        doc_name = Path(document_path).stem

        pdf_doc = fitz.open(document_path)
        extracted_data = []

        for page_num in range(len(pdf_doc)):
            # page = pdf_doc[page_num]
            page = cast(Any, pdf_doc[page_num])
            page_text = page.get_text()
            image_list = page.get_images()

            for img_index, img in enumerate(image_list):
                xref = img[0]
                pix = fitz.Pixmap(pdf_doc, xref)

                # Skip very tiny images (like dots or background)
                if pix.width < 10 or pix.height < 10:
                    pix = None
                    continue

                # ✅ Convert to RGB if alpha channel exists or it's not grayscale/RGB
                if pix.alpha or pix.colorspace not in (fitz.csGRAY, fitz.csRGB):
                    pix_converted = fitz.Pixmap(fitz.csRGB, pix)
                    img_data = pix_converted.tobytes("png")
                    pix_converted = None
                else:
                    img_data = pix.tobytes("png")

                # Save the image
                img_filename = f"{doc_name}_pg{page_num + 1}_img{img_index + 1}.png"
                img_path = os.path.join(output_dir, img_filename)
                blob.upload_file(service=blob.authenticate(), file_path=img_path, folder_name="DocumentImages")

                with open(img_path, "wb") as img_file:
                    img_file.write(img_data)

                # Get context around the image
                try:
                    img_rect = page.get_image_bbox(img)
                    context_before, context_after = self.__get_text_context_around_image__(
                        page, img_rect, page_text, context_chars=500
                    )
                except:
                    context_before, context_after = self.__get_page_text_context__(
                        page_text, len(image_list), img_index, context_chars=500
                    )

                # Store extraction info
                extraction_info = {
                    "image_filename": img_filename,
                    "image_path": img_path,
                    "page_number": page_num + 1,
                    "image_number": img_index + 1,
                    "context_before": context_before,
                    "context_after": context_after,
                    "image_dimensions": (pix.width, pix.height),
                }
                extracted_data.append(extraction_info)

                # Cleanup
                pix = None

        pdf_doc.close()
        return extracted_data

    def create_document_summary(self, llm: LLM, document_path) -> str | None:
        document = fitz.open(document_path)
        cleaned_text = ""

        for page_num in range(len(document)):
            # page = document[page_num]
            page = cast(Any, document[page_num])
            blocks = page.get_text("blocks") 
            blocks.sort(key=lambda b: (b[1], b[0]))  

            text = " ".join([b[4].strip() for b in blocks if b[4].strip()])
            cleaned_text += self.__clean_text__(text) 

        summary = llm.generate_document_summary(text=cleaned_text)
        return summary    

    def query_images_with_text(self, query: str, namespace="images", top_k=5):
        device = "cuda" if torch.cuda.is_available() else "cpu"
        query_vector = self.__get_clip_embedding__(text=query)

        log.log_event("SYSTEM", "Querying Images in PineconeDB")
        results = self.index_images.query(
            namespace=namespace,
            vector=query_vector,
            top_k=top_k,
            include_metadata=True
        )
        results = cast(Dict[str, Any], results)
        return results["matches"][0]["metadata"]["source"]

    def query_text(self, user_query, namespace="documents", top_k=3):
        log.log_event("SYSTEM", "Querying Text in PineconeDB")
        
        query_dict = {
            "inputs": {
                "text": user_query
            },
            "top_k": top_k
        }
        typed_query = cast(Dict[str, Any], query_dict)
        results = self.index.search(
            namespace=namespace,
            query=cast(Any, typed_query),
            fields=["text"]
        )

        texts = [hit["fields"]["text"] for hit in results["result"]["hits"]]
        return "\n".join([f"Answer {i+1}: {item} \n" for i, item in enumerate(texts)])