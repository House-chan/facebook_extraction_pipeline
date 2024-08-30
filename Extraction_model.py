from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
import os
from langchain_core.pydantic_v1 import BaseModel, Field
from typing import Tuple, List, Optional
from langchain_community.embeddings.openai import OpenAIEmbeddings
import dotenv
from dateutil import parser

dotenv.load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
llm = ChatOpenAI(model="gpt-4o-mini", api_key=api_key)
embeddings = OpenAIEmbeddings(api_key=api_key)

class Entities(BaseModel):
     """Identifying information about entities."""
     post_type: str = Field(
          ...,
          description="Determine the type of post in real estate in ต้องการขายบ้าน, ต้องการซื้อบ้าน, etc.",
     )
     house_number: Optional[str] = Field(
          ...,
          description="Extract only house number from the provided information that appear in the text make sure it's house number.(e.g., apartment number, house number within a development), if there is no house number leave empty",
     )
     property_type: str = Field(
          ...,
          description="Determine the type of property with name.",
     )
     area_wah: float = Field(
          ...,
          description="Extract the area of the property and convert into square wah (ตารางวา). if there are none said 0",
     )
     area_meter: float = Field(
          ...,
          description="Extract the area of the property and convert into square meters (ตารางเมตร). if there are none said 0",
     )
     facing:Optional[str] = Field(
          ...,
          description="Determine the direction the property faces (e.g., North, South, East, West).",
     )
     bedrooms:Optional[int] = Field(
          ...,
          description="Extract the number of bedrooms.",
     )
     bathrooms:Optional[int] = Field(
          ...,
          description="Extract the number of bathrooms",
     )
     price:float = Field(
          ...,
          description="Extract the listed price of the property make sure it the price not rental rate.",
     )
     rental:str = Field(
          ...,
          description="Extract range of the monthly rental rate If the information indicates the property is for rent, if not leave with 0.",
     )
     status:str = Field(
          ...,
          description="Determine if the property is available(ว่าง), sold(ขายแล้ว), under construction(อยู่ในระหว่างการก่อสร้าง), transfer(จองแล้ว), show unit(ตัวจัดแสดง(ว่าง)) etc. in thai language.",
     )
     img_url:Optional[str] = Field(
          ...,
          description="Extract the URL link to the photos of the unit.",
     )
     location:str = Field(
          ...,
          description="Extract the precise address or description of the property's location.",
     )
     year_built:int = Field(
          ...,
          description="Extract the year the property was constructed, if there are none leave it 0.",
     )
     facility:str = Field(
          ...,
          description="List the facility available with the property (e.g., pool, gym, parking).",
     )
     transport:str = Field(
          ...,
          description="Describe the access to public transportation (e.g., walking distance to BTS, bus routes nearby).",
     )
     contract:str = Field(
        ...,
        description="List any information about the contract information(e.g., phone number, Line ID, Facebook name).",
    )
     environment:str = Field(
          ...,
          description="Summarize any information related to pollution levels, noise, proximity to temples, railways, etc.",
     )
     add_info:str = Field(
          ...,
          description="Summarize any additional relevant details not covered in other fields(e.g. pet, name of real estate, Common area fee), exclude transportation in this field.",
     )

prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are extracting house entities information from the social media post and extracting in thai language.",
        ),
        (
            "human",
            "Use the given format to extract information from the following"
            "input: {question}",
        ),
    ]
)
entity_chain = prompt | llm.with_structured_output(Entities)

def get_entities(text: str, date, img_list) -> dict:
     query = dict(entity_chain.invoke(text))
     #? check year_built is empty
     if "ต้องการขายบ้าน" == query["post_type"]:
          if not query["year_built"]:
               del query["year_built"]
          
          #? check rental is empty
          if query["rental"] == 0.0:
               del query["rental"]

          #? check bathroom or bedrooms, one of them is empty
          if query["bathrooms"] == 0 and query["bedrooms"] != 0:
               del query["bathrooms"]
          elif query["bathrooms"] != 0 and query["bedrooms"] == 0:
               del query["bedrooms"]

          embeded = embeddings.embed_query(str(query))
          #? check listing_date is empty
          query["listing_date"] = date
          query["img_url"] = img_list
          query["embedding"] = embeded

     return query