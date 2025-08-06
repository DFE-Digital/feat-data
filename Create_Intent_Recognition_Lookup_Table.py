import os 
from azure.search.documents.indexes.models import (
        SimpleField,
        SearchFieldDataType,
        SearchableField,
        SearchField,
        VectorSearch,
        HnswAlgorithmConfiguration,
        VectorSearchProfile,
        SemanticConfiguration,
        SemanticPrioritizedFields,
        SemanticField,
        SemanticSearch,
        SearchIndex,
        AzureOpenAIVectorizer,
        AzureOpenAIVectorizerParameters
        
)
import json
import pandas as pd 
import math
from datetime import datetime
import os
from openai import AzureOpenAI
import pandas as pd
from azure.search.documents.indexes import SearchIndexClient
import time
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

SSAT1s=["Health, Public Services and Care",
            "Science and Mathematics",
            "Agriculture, Horticulture and Animal Care",
            "Engineering and Manufacturing Technologies",
            "Construction, Planning and the Built Environment",
            "Digital Technology",
            "Retail and Commercial Enterprise",
            "Leisure, Travel and Tourism",
            "Arts, Media and Publishing",
            "History, Philosophy and Theology",
            "Social Sciences",
            "Languages, Literature and Culture",
            "Education and Training",
            "Preparation for Life and Work",
            "Business, Administration and Law"
            ]
    
SSAT2s=[
        "Medicine and dentistry",
        "Nursing, and subjects and vocations allied to medicine",
        "Health and social care",
        "Public services",
        "Child development and well being"
        ,"Science"
        ,"Mathematics and statistics",
        "Agriculture",
        "Horticulture and forestry",
        "Animal care and veterinary science",
        "Environmental conservation",
        "Engineering",
        "Manufacturing technologies",
        "Transportation operations and maintenance",
        "Architecture",
        "Building and construction",
        "Urban, rural and regional planning",
        "Digital technology (practitioners)",
        "Digital technology (users)",
        "Retailing and wholesaling",
        "Warehousing and distribution",
        "Service enterprises",
        "Hospitality and catering",
        "Sport, leisure and recreation",
        "Travel and tourism",
        "Performing arts",
        "Crafts, creative arts and design",
        "Media and communication",
        "Publishing and information services",
        "History",
        "Archaeology and archaeological sciences",
        "Philosophy",
        "Theology and religious studies",
        "Geography",
        "Sociology and social policy",
        "Politics",
        "Economics",
        "Anthropology",
        "Languages, literature and culture of the British isles",
        "Foreign languages, literature and culture", # Originally the SSAT2 defs have "Other" and I think that changing the word to "Foreign" makes more sense in context
        "Linguistics",
        "Teaching and lecturing",
        "Direct learning support",
        "Preparation for Life and Work",
        "Foundations for learning and life",
        "Preparation for work",
        "Accounting and finance",
        "Administration",
        "Business management",
        "Marketing and sales",
        "Law and legal services"]


list_terms=SSAT1s+SSAT2s


azenv=os.getenv('AZURE_CIP')
#AZ_ENDPOINT_LM=os.getenv('LLM_ENDPOINT')
AZ_ENDPOINT_EMBEDDING=os.getenv('EMBEDDING_ENDPOINT')

AZURE_AI_SEARCH_ADMIN_KEY=os.getenv('ADMIN_KEY')
#print(azenv,"\n",AZ_ENDPOINT_EMBEDDING,"\n",AZURE_AI_SEARCH_ADMIN_KEY)
Embeddingclient = AzureOpenAI(
        api_key=azenv,  
        api_version="2024-08-01-preview",
        azure_endpoint=AZ_ENDPOINT_EMBEDDING)   
stime=time.time()
client_response=Embeddingclient.embeddings.create(input=list_terms,model='text-embedding-ada-002')
etime=time.time()
print("Time elapsed: {}s".format(round(etime-stime),4))

intent_embedding=[item.embedding for item in client_response.data]
print(len(intent_embedding),len(list_terms))
dict_embeddings={
    'IntentRecognitionSearchTerms':list_terms,
    'Embeddings':intent_embedding
}
with open("jsons/IntentRecognition_List_Terms_StructOfArray.json",'w+') as jf:    
    json.dump(dict_embeddings,jf,indent=4)

# Convert Struct (Dictionary) of Array (List) / SoA into Array of struct (AoS)
docs_to_upload=[]
for it in range(0,len(list_terms)):
    doc={}
    doc['IntentRecognitionSearchTerm']=list_terms[it]
    doc['IntentRecognitionSearchTermVector']=intent_embedding[it]
    doc['id']='Document_{}'.format(it)
    docs_to_upload.append(doc)


# Now got the list of docs, now we define the index.

def Create_Index(docs:dict={},indexname:str='intentrecognition'):    
    index_client=SearchIndexClient(
        endpoint=os.environ['AZURE_AI_SEARCH_URL'],credential=AzureKeyCredential(os.environ['ADMIN_KEY']))
    
    #establish fields for the index, searchable use keyword search, search use vectors, simple can be sortable filterable or facetable
    fields=[SimpleField(name="id", type=SearchFieldDataType.String, key=True, sortable=True, filterable=True, facetable=True),            
            SearchableField(name="IntentRecognitionSearchTerm", type=SearchFieldDataType.String),
            SearchField(name="IntentRecognitionSearchTermVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),searchable=True, vector_search_dimensions=1536, vector_search_profile_name="myHnswProfile"),
            ]

    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="myHnsw"
            )
        ],
        profiles=[
            VectorSearchProfile(
                name="myHnswProfile",
                algorithm_configuration_name="myHnsw",
            )
        ])
    
    index=SearchIndex(name=indexname,fields=fields ,vector_search=vector_search)
    result=index_client.create_or_update_index(index)

    search_client=SearchClient(endpoint=os.environ['AZURE_AI_SEARCH_URL'],
            index_name=indexname,
            credential=AzureKeyCredential(os.environ['ADMIN_KEY']))
    print(len(docs))
    search_client.upload_documents(docs)
    print(f"{result.name} created")

Create_Index(docs_to_upload,'intentrecognition')