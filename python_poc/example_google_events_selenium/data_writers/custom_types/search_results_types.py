import typing
import datetime
from enum import Enum

class SearchQuery(typing.Dict):
   id: str
   full_query_id: str
   query: str 
   full_query: str
   start_date: str
   end_date: str
   site: str
   filetype: str
   runtime: datetime.datetime

class SearchQueryResult(typing.Dict):
   id: str
   full_query_id: str
   title: str
   url: str
   published_date: str
   result_html: str
   modal: str

class SearchResultType(Enum):
   BASIC_LINK = 1
   PDF_LINK = 2
   YOUTUBE_VIDEO = 3