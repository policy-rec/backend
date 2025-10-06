from openai import OpenAI, AuthenticationError
from typing import cast, List, Dict, Any
from dotenv import load_dotenv
from logger import Logger
import base64
import json
import os

load_dotenv()
log = Logger()
IMAGE_FOLDER = os.environ.get("IMAGE_FOLDER")

class LLM:
    def __init__(self) -> None:
        try:
            self.client = OpenAI(api_key=os.environ.get("LLM_API_KEY"))
            self.client.models.list()
            log.log_event("SYSTEM", "LLM API Connected")
        except AuthenticationError as e:
            log.log_event("SYSTEM", "LLM API Not Connected")

        self.system_prompt_v = """
            You are a classification assistant trained to evaluate user inputs. Your role is to determine whether a given user input falls into one of three categories:
            1. **Valid RAG Question**: A query related to the provided description and domain, which is suitable for further processing by the RAG system.
            2. **Greeting**: Polite or casual expressions, such as "Hi," "Hello," or "How are you?"
            3. **Off-Topic**: Inputs unrelated to the described domain.

            You will be provided with:
            - A brief description of the domain or system context.
            - The latest user input.
            - The last few messages exchanged between the user and the bot (up to 10 messages).

            Use this conversation history to better understand whether the current input is a follow-up to a previous valid question, part of a greeting, or entirely off-topic.

            ### Guidelines:
            - If the current user input contains only polite or casual phrases (e.g., greetings), classify it as **Greeting**.
            - If the input is a continuation or follow-up to a previous domain-relevant discussion, classify it as a **Valid RAG Question**, even if it’s short or depends on previous context.
            - If the input is unrelated to the described domain, or shows no relevance to prior messages or the domain context, classify it as **Off-Topic**.
            - The user may switch between English and Roman Urdu (Urdu written using English letters, e.g., "aap kaise ho").
            - Be concise and avoid unnecessary elaboration. Output one of the following:
            - "Valid RAG Question"
            - "Greeting"
            - "Off-Topic"

            ### Domain Description:

        """
        self.system_prompt_r = """
            You are a friendly and professional chatbot designed to guide and assist users with their queries in a clear, step-by-step, and approachable manner. Your behavior depends on the classification results provided by a validator LLM.
            Detect the language of the user's input.
                - If the user types in Roman Urdu (e.g. Kia haal hai? i.e. How are you?), respond in Urdu.
                - If the user types in English or any other language, respond in English only.

            Do not mix the two languages in your replies. The user may switch between the two languages on every sentence, and so should you, based on user's input. Keep your tone friendly and conversational.
            Here's how you should respond based on the validator's output:

            Context:
            The validator LLM classifies user input into three categories:
                - Valid RAG Question: The input is a relevant question for the system's domain.
                - Greeting: The input is a greeting, casual expression, or a polite remark (e.g., "Hi," "Hello," or "Thank you").
                - Off-Topic: The input is unrelated to the system's domain.

            The validator's classification will be provided to you alongside any relevant information for valid questions. Your job is to craft an appropriate response based on the classification.

            Response Guidelines:
                For Valid RAG Questions:
                    Keep answers concise and easy to follow. Use step-by-step explanation only if needed or if user asks for more detail.
                    Simplify complex topics into short, clear points. Add extra detail or examples only if user requests it.
                    Use bullet points, numbering, or clear formatting to structure responses for readability.
                    The "RAG Answer" may contain multiple top-matching chunks of text retrieved from the document database.
                    These chunks may not be in perfect order, may overlap in content, or contain partial information.
                    Your task is to read all the chunks, extract relevant parts, and then reconstruct a complete, ordered, and easy-to-understand answer for the user.
                    If a user-uploaded image is present, consider its contents as part of the question context.
                    If a RAG-selected image is present, use it to supplement your explanation or to visualize parts of your answer.
                    If the chunks contain steps, processes, or sequences, try to arrange them step-by-step logically.
                    Avoid repeating identical sentences from different chunks.

                    Example:
                        Input: "How does machine learning work?"
                        Answer Provided: "Machine learning involves training algorithms to recognize patterns in data."

                        Your Response:
                            Machine learning trains algorithms to find patterns in data.
                            In simple terms: you give it data → it learns patterns → it predicts on new data.
                            Would you like me to explain the steps in more detail?

            For Greeting Messages:
                Respond in a warm, friendly, and polite manner. Add a touch of positivity to make users feel welcome.
                
                Example:
                    Input: "Hi!"
                    Your Response: "Hello there! How can I assist you today? 😊"

            For Off-Topic Queries:
                Politely address and explain that the input is outside the domain.
                Encourage the user to rephrase their query or ask a domain-related question.
                Suggest examples or topics the system can handle.

                Example:
                    Input: "What's the weather like?"
                    Your Response: "That’s a good question! But I focus on [domain-related queries]. For example, I can help with [example topics]. Want to try one of those?"

            Key Behaviors:
                Maintain a professional yet friendly tone.
                Tailor responses to the user’s input classification.
                Encourage exploration and learning where appropriate.
                Always aim to guide the user toward a helpful and engaging interaction.
                Interpret images (whether user-uploaded or RAG-provided) thoughtfully and incorporate them into your reasoning and responses.
        """
        self.system_prompt_i = """
            You are a concise and precise image describer designed to interpret and summarize the content of images based on their visual features and surrounding context.

            Your task is to analyze the provided image and optionally use the surrounding text (if relevant) to create a brief, 7-line description of what the image is showing.
                - Only use the text context if it clearly refers to the image or helps explain its content.
                - If the surrounding context is unrelated, do not try to refute or mention it in the description. Just ignore it and describe the image on its own.

            The description must:
                - Be directly relevant to the image, and optionally enhanced by the document context.
                - Use terminology and phrasing likely to appear in technical help queries.
                - Clearly mention key visual features, objects, and actions shown in the image.
                - Avoid vague, abstract, or subjective terms (e.g., “clear”, “useful”, “important”).
                - Stick to one concise sentence per line, total max 7 lines.

            Example 1 (Context and image are related):
                - Context Before: "The chart illustrates the yearly sales growth across various regions."
                -Context After: "As evident from the graph, Region B has shown the most consistent improvement."
                - Image: (Line chart showing sales growth for different regions over 5 years.)
                - Your Description:
                A line chart comparing yearly sales growth across regions over 5 years.
                Region B shows the most consistent increase.
                Other regions display fluctuations in growth.
                The chart uses colored lines to differentiate each region.
                Time is plotted along the x-axis and sales growth on the y-axis.

            Example 2 (Context is unrelated):
                - Context Before: "Be sure to check battery safety guidelines in the next section."
                - Context After: "Improper handling of power cells can lead to overheating."
                - Image: (A stealth aircraft flying above farmland.)
                - Your Description:
                A stealth aircraft flying at an angle above a landscape.
                The aircraft has a sleek, angular design with visible markings.
                Fields and farmland are visible below the aircraft.
                The sky is clear, suggesting daytime.
                The aircraft is jet-powered with a pointed nose and sharp wings.
        """
        self.system_prompt_s = """
        You are a professional document summarizer. Your task is to read a set of text from a PDF document and generate a clear, concise, and accurate summary of the document.

        Instructions:
        - Combine the information into a cohesive summary.
        - Focus on the key purpose, structure, sections, and instructions given in the document.
        - Eliminate repetition caused by overlapping text.
        - Maintain a professional and informative tone.
        - Do NOT include metadata like image or page numbers in your response.
        - The entire summary should only be the GIST of the entire document, to only provide context of the document to a LLM.
        """

        self.model_v = "gpt-4.1-mini-2025-04-14"
        self.model_r = "gpt-4.1-mini-2025-04-14"
        self.model_i = "gpt-4o"
        self.model_s = "gpt-4o"
        
        self.temperature_v = 0.3
        self.temperature_r = 0.4
        self.temperature_i = 0
        self.temperature_s = 0.5

    def __encode_image__(self, image_path):
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")

    def __format_RLLM_input__(self, document_context, user_input, vllm_classification, rag_ans, convo) -> str:
        return f"[Context of the Documents]:\n{document_context}\n\n[User Input]:\n{user_input}\n\n[Validator LLM Classification]:\n{vllm_classification}\n\n[RAG Answer]:\n{rag_ans}\n\n[Conversational History]:\n{convo}"

    def generate_image_description(self, context, image_path):
        img_b64 = self.__encode_image__(image_path)

        try:
            response = self.client.chat.completions.create(
                model=self.model_i,
                temperature=self.temperature_i,
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt_i
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": f"Describe the image with the following context: {context}"
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{img_b64}"
                                }                                                       
                            }
                        ]
                    }
                ],
            )
        except Exception as e:
            log.log_event("SYSTEM", f"RESPONSE FAILED - Image Description LLM - {e}")
            return None
        
        if response:
            return response.choices[0].message.content
        else:
            log.log_event("SYSTEM", "NO RESPONSE GENERATED - Image Description LLM")
            return None

    def generate_document_summary(self, text):
        # for token in tokens:
        try:
            response = self.client.chat.completions.create(
                model=self.model_s,
                temperature=self.temperature_s,
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt_s
                    },
                    {
                        "role": "user",
                        "content": text
                    }
                ],
            )
        except Exception as e:
            log.log_event("SYSTEM", f"RESPONSE FAILED - Document Summary LLM - {e}")
            return None

        if response:
            return response.choices[0].message.content
        else:
            log.log_event("SYSTEM", "NO RESPONSE GENERATED - Document Summary LLM")
            return None

    def respond(self, user_input, user_image_path = None, rag_image_path = None):
        content: list = [] 
        content.append({"type": "text", "text": user_input})

        if user_image_path:
            user_img_b64 = self.__encode_image__(user_image_path)
            content.append({"type": "text", "text": "Here is an image uploaded by the user describing their problem:"})
            content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{user_img_b64}"}})

        if rag_image_path:
            rag_img_b64 = self.__encode_image__(rag_image_path)
            content.append({"type": "text", "text": "Here is a system-retrieved image that may help answer the question:"})
            content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{rag_img_b64}"}})

        try:
            if user_image_path or rag_image_path:
                response = self.client.chat.completions.create(
                    model=self.model_r,
                    temperature=self.temperature_r,
                    messages=[
                        {
                            "role": "system",
                            "content": self.system_prompt_r
                        },
                        {
                            "role": "user",
                            "content": content
                        }
                    ],
                )
            else:
                response = self.client.chat.completions.create(
                    model=self.model_r,
                    temperature=self.temperature_r,
                    messages=[
                        {
                            "role": "system",
                            "content": self.system_prompt_r
                        },
                        {
                            "role": "user",
                            "content": user_input
                        }
                    ],
                )
        except Exception as e:
            log.log_event("SYSTEM", f"RESPONSE FAILED - Responder LLM - {e}")
            return None
        
        if response:
            return response.choices[0].message.content
        else:
            log.log_event("SYSTEM", "NO RESPONSE GENERATED - Responder LLM")
            return None

    def validate(self, user_input, document_context, user_convo):
        try:
            response = self.client.chat.completions.create(
                model=self.model_v,
                temperature=self.temperature_v,
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt_v + document_context
                    },
                    {
                        "role": "user",
                        "content": f"[[Conversation History]]:\n{user_convo}\n[[User Input]]:\n{user_input}"
                    }
                ],
            )
        except Exception as e:
            log.log_event("SYSTEM", f"RESPONSE FAILED - Validator LLM - {e}")
            return None
        
        if response:
            return response.choices[0].message.content
        else:
            log.log_event("SYSTEM", "NO RESPONSE GENERATED - Validator LLM ")
            return None


####################################################################
####################################################################
### TESTING ###

# llm = LLM()
# db = DBToolKit()
# doc = Document()
# print(llm.generate_image_description("helicopter flying from hospital", './images/0db5d2805318e2251e784638d4d3063bfed8a491_2_1380x862.jpeg'))
# print(llm.generate_image_description("on a stealth surveillence mission", './images/Screenshot (651).png'))
# # print(llm.validate("Hi i need help!"))
# while True:
#     user = input("\nUser: ")
#     v_input = llm.validate(user, db.get_all_document_desc())
#     print("V_Bot: ", v_input)
#     print("R_Bot: ", llm.respond(llm.__format_RLLM_input__(db.get_all_document_desc(), user, v_input, doc.query_text(user_query=user), db.__format_chat_history__(db.get_user_chats(1))), image_path=os.path.join(IMAGE_FOLDER, doc.query_images_with_text(user))))

### TESTING ###
####################################################################
####################################################################