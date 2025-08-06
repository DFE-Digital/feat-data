import azure.functions as func
import datetime
import json
import logging

import json 
import pyodbc 
import sqlalchemy
import pandas as pd
import requests
import aiohttp
import asyncio
# LOAD CREDS FROM FILE
print("LOADING API KEYS FROM EXTERNAL FILE NOT IN REPO")
jf=open(r'C:\Users\manthony2\OneDrive - Department for Education\Documents\ESFA_PROJECT\Workdir\API_KEYS\API_KEYS.json')
api_keys=json.load(jf)
jf.close()

print("Hello : {}".format(api_keys['USERNAME']))
SERVER=api_keys['SERVER']
USERNAME=api_keys['USERNAME']

DATABASE=api_keys['DATABASE']

AUTHENTICATION=api_keys['AUTHENTICATION']

DRIVER="ODBC Driver 18 for SQL Server"

print("STARTING CONNECT")
connection = 'DRIVER='+DRIVER+';SERVER='+SERVER+';DATABASE='+DATABASE+'; authentication='+AUTHENTICATION+'; Encrypt=Yes; UID='+USERNAME+';'

conn_string=print(connection)
conn=pyodbc.connect(connection)


print("GOT CONN")

engine = sqlalchemy.create_engine('mssql+pyodbc://@' + SERVER + '/' + DATABASE + '?authentication='+AUTHENTICATION+'&driver=ODBC+Driver+18+for+SQL+Server&uid='+USERNAME+'')
conn_string_sqlalchemy='mssql+pyodbc://@' + SERVER + '/' + DATABASE + '?authentication='+AUTHENTICATION+'&driver=ODBC+Driver+18+for+SQL+Server&uid='+USERNAME+''
conn=engine
print(conn_string_sqlalchemy)

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 9 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def ApprenticeshipDataLoaderFeed(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function executed.')
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    

    
    df_test=pd.read_sql('SELECT TOP(10) * FROM ASData_PL.Va_Vacancy',con=engine)
    if(len(df_test)>0):
        logging.info("TEST QUERY: ROWS OBTAINED, ALL TEST OK")

    currdate=datetime.datetime.now().date()
    diffdate=currdate+datetime.timedelta(days=-1)
    


    sql_code="""
    SELECT  
    VacancyId,
    VacancyReferenceNumber,
    VacancyTitle,
    VacancyPostcode,
    VacancyAddressLine1,
    VacancyAddressLine3,
    VacancyAddressLine4,
    VacancyAddressLine5,
    VacancyTown,
    SkillsRequired,
    QualificationsRequired,
    PersonalQualities
    EmployerFullName,
    LegalEntityName,
    ProviderFullName,
    ProviderTradingName,
    ApprenticeshipType,
    VacancyShortDescription,
    VacancyDescription,
    NumberOfPositions,
    SectorName,
    FrameworkOrStandardID,
    FrameworkOrStandardLarsCode,
    FrameworkOrStandardName,
    EducationLevel,
    WageText,
    WageUnitDesc,
    HoursPerWeek,
    DurationTypeDesc,
    ApplicationClosingDate,
    ExpectedStartDate,
    DatePosted
  
    FROM
    ASData_PL.Va_Vacancy
    WHERE [VacancyStatus]='Live'
    AND
     DatePosted>=TRY_CAST('{}' AS date)
    """.format(diffdate)
    df_vac=pd.read_sql(sql_code,engine)
    logging.info(f"Real query: Number of rows: {len(df_vac)}")

    def GetFAAURL(refid=9999):
        output_url=f"https://www.findapprenticeship.service.gov.uk/apprenticeship/VAC{refid}"
        return output_url
    
   

    async def CheckStatus(x):
        # Code to check that the URL is live and that the HTML page existed (i.e. the FAA site should be running concurrently)
        url = x
        #if(CheckStatus.counter%10==0):
        #    #time.sleep(1)
        #    print(f"Processed {CheckStatus.counter} rows")

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    logging.debug("GOT HTTP RESPONSE from :{}".format(url))
                    timestamp=datetime.datetime.now()
                    return response.status,response.content,timestamp            
            except Exception as E:
                print("EXCEPTION: {}".format(E))
                await asyncio.sleep(0.00001)
                return  pd.NA,pd.NA,pd.NA

    async def ManageURLChecking(urls=[]):
        list_tasks=[CheckStatus(link) for link in urls]
        results=await asyncio.gather(*list_tasks)
        return results
    logging.info("NOW GET URL")
    df_vac['LOOKUPURL']=df_vac['VacancyReferenceNumber'].apply(lambda x: GetFAAURL(x))


    df_vac[['HTTP_STATUS','HTML_SITE_PAGE','RETRIEVETIME']]=asyncio.run(ManageURLChecking(df_vac['LOOKUPURL'].to_list()))
    logging.info("GOT STATUS")
    df_vac.to_csv(r"C:\Users\manthony2\OneDrive - Department for Education\Documents\GitHub\feat-data\DataLoader_Apprenticeships\OutputData\Apprenticeship_Data_Timestamp_{}.csv".format(str(currdate).replace(":","_")))
    logging.info("Finished job, now restarting the wait loop")