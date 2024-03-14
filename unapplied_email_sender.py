import pandas as pd
import gspread as gs
from google.oauth2 import service_account
import numpy as np
import streamlit as st
import requests
import datetime as dt
import json
pd.options.display.float_format = "{:,.2f}".format


st.set_page_config('Unapplied Reachout',page_icon=':e-mail:',layout='wide')
st.title(':red[Email] Reachout Unapplied :e-mail:')

reports = st.file_uploader('Upload Uncategorized asset Report',accept_multiple_files=True)


# Google sheets ids Uncategorized report
ws_id = 0
n2_id = 1996569107

# Google sheets ID customer contacts
contacts_ws = 354109062

# Google sheets ID AR assigned contacts
ws_ar_associate = 612191044

# Google sheets ID Logs
eft_log_ws_id = 2020060949
cash_log_ws_id = 1792079758
checks_log_ws_id = 813380796

front_teammate_id = {'justin@nabis.com':'tea_2c5aj',
'filip.gacic@nabis.com':'tea_3bksr',
'milos@nabis.com':'tea_2hyh7',
'ana.rondero@nabis.com':'tea_39v63',
'dustin.kinjerski@nabis.com':'tea_3dd6z',
'aron.rivero@nabis.com':'tea_3beiz',
'gabrielle.maschke@nabis.com':'tea_31n2j',
'nela.eric@nabis.com':'tea_3dd8r',
'liliana.bravo@nabis.com':'tea_3b7bf',
'stephany.martinez@nabis.com':'tea_2yzgr',
'darko.djokic@nabis.com':'tea_3gevf',
'dragana.rankovic@nabis.com':'tea_3gex7'
}




def read_gs_byID(gs_ID,ws_ID,range_columns):
    scope = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
    credentials = service_account.Credentials.from_service_account_info(st.secrets['gcp_service_account'],scopes=scope)
    client = gs.authorize(credentials=credentials)
    google_sheet = client.open_by_key(gs_ID)
    tab_name = google_sheet.worksheets()
    sheet = google_sheet.get_worksheet_by_id(ws_ID)
    data = sheet.batch_get([range_columns])
    return data


def update_gs_byID(gs_ID,df_values,sheet_name,range_to_update):
    scope = ['https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets']

    # Creating the credentials variable to connect to the API
    credentials = service_account.Credentials.from_service_account_info(st.secrets['gcp_service_account'],scopes=scope)

    # Passing the credentias to gspread
    client = gs.authorize(credentials=credentials)

    # Opening Google sheet using the ID
    google_sheet = client.open_by_key(gs_ID)

    # Clear sheet values
    # sheet = google_sheet.get_worksheet_by_id(ws_ID)

    worksheet = client.open_by_key(gs_ID).worksheet(sheet_name)
    
    data = pd.DataFrame(df_values)
    data = data.astype(str)          
    # Clear the existing data from the worksheet
    worksheet.batch_clear([range_to_update])
    
    worksheet.update([data.columns.values.tolist()] + data.values.tolist())


def paperwork_data(data,data_aging):
        
    update_gs_byID(st.secrets['gs_ID']['uncategorized'],data,sheet_name='1490_Uncategorized',range_to_update='A1:Q')

    # Reading Logs
    eft_data = read_gs_byID(st.secrets['gs_ID']['eft_log_ID'],eft_log_ws_id,'A:P')
    eft_df = pd.DataFrame(eft_data[0][1:],columns=eft_data[0][0])
    eft_df = eft_df[['Date','Transfer Amount','Payment Reference','Retailer']].copy()
    eft_df['Transfer Amount'] = eft_df['Transfer Amount'].replace('-',np.nan)
    eft_df['Transfer Amount'] = eft_df['Transfer Amount'].replace('',np.nan)
    eft_df.dropna(inplace=True)
    eft_df.rename(columns={'Transfer Amount':'Amount'},inplace=True)

    cash_data = read_gs_byID(st.secrets['gs_ID']['cash_log_ID'],cash_log_ws_id,'A:P')
    cash_df = pd.DataFrame(cash_data[0][1:],columns=cash_data[0][0])
    cash_df = cash_df[['Date','Amount','Payment Reference','Retailer']]
    cash_df['Amount'] = cash_df['Amount'].replace('-',np.nan)
    cash_df['Amount'] = cash_df['Amount'].replace('',np.nan)
    cash_df.dropna(inplace=True)

    checks_data = read_gs_byID(st.secrets['gs_ID']['check_log_ID'],checks_log_ws_id,'A:Q')
    checks_df = pd.DataFrame(checks_data[0][1:],columns=checks_data[0][0])
    checks_df = checks_df[['Date','Check Amount','Check Number','Retailer']]
    checks_df['Check Amount'] = checks_df['Check Amount'].replace('-',np.nan)
    checks_df['Check Amount'] = checks_df['Check Amount'].replace('',np.nan)
    checks_df.dropna(inplace=True)
    checks_df.rename(columns={'Check Amount':'Amount','Check Number':'Payment Reference'},inplace=True)

    logs_2022_data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],1559978804,'A:D')
    logs_2022_df = pd.DataFrame(logs_2022_data[0][1:],columns=logs_2022_data[0][0])

    consolidated_logs = pd.concat([logs_2022_df,eft_df,cash_df,checks_df])
    consolidated_logs_dict = dict(zip(consolidated_logs['Payment Reference'],consolidated_logs['Amount']))
    consolidated_logs['Retailer_ID'] = np.where(len(consolidated_logs['Retailer']) > 4, consolidated_logs['Payment Reference'] + consolidated_logs['Retailer'].str.slice(stop=4), consolidated_logs['Payment Reference'] + consolidated_logs['Retailer'])
    checks_dict = dict(zip(consolidated_logs['Retailer_ID'] ,consolidated_logs['Amount']))


    # Reading Uncategorized info from google sheets
    data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],ws_id,'A:Q')
    # Reading data into a pandas DF
    df = pd.DataFrame(data[0][1:],columns=data[0][0])
    df['Customer'] = df['Customer'].str.title()

    # Reading Uncategorized data from N2 to get the correct Payment Dates
    n2_data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],n2_id,'A:P')
    # Reading data into a pandas DF
    df_n2 = pd.DataFrame(n2_data[0][1:],columns= n2_data[0][0])
    #Create Dictionary
    df_n2_dict = dict(zip(df_n2['Num'] , df_n2['Date']))
    # Create Column Payment Date
    df['Payment_Date'] = np.where(df['Num'].isin(df_n2_dict) , df['Num'].map(df_n2_dict),df['Date'])

    # filtering Data
    df = df.loc[df['Class']=='OP - Unapplied'].copy()
    # Cleaning data and converting to number
    df['Amount'] = df['Amount'].apply(lambda x: x.replace(',',''))
    df['Amount'] = pd.to_numeric(df['Amount'])
    # Creating new column payment type
    df['Payment Type'] = np.where(df['Num'].str.contains('Cash'),'Cash',np.where(df['Num'].str.contains('EFT'),'EFT','Check'))
    # Changing column names
    df.rename(columns={'Num':'Payment Ref','Location':'Issue Reason'},inplace=True)
    # Grouping the data 
    df_gpd = df.groupby(['Customer','Payment Ref','Payment_Date','Issue Reason','Payment Type']).agg({'Amount':'sum'}).reset_index()
    # Filtering out data with out info
    df_gpd = df_gpd.loc[df_gpd['Customer']!=''].copy()
    # Getting only the retailer name, removing Retailer Group:
    df_gpd['Customer_Name'] = df_gpd['Customer'].apply(lambda x: x.split('Group:')[-1].strip())


    # Reading gs file to get the AR associate
    ar_associate = read_gs_byID(st.secrets['gs_ID']['ar_associate_gs'],ws_ar_associate,'A:N')
    # Crreating Dataframe
    df_ar_rep = pd.DataFrame(ar_associate[0][1:],columns= ar_associate[0][0])
    df_ar_rep['Retailer'] = df_ar_rep['Retailer'].str.title()
    # Dictionary of AR emails contacts
    ar_rep_dict = dict(zip(df_ar_rep['Retailer'],df_ar_rep['Account Assignments']))



    # Reading in google sheets with contactas emails
    df_conctacts_data = read_gs_byID(st.secrets['gs_ID']['contacts_gs'],contacts_ws,'A:F')
    # Reading data into a pandas GS
    df_conctacts = pd.DataFrame(df_conctacts_data[0][1:],columns=df_conctacts_data[0][0])
    df_conctacts['name'] = df_conctacts['name'].str.title()
    # Creating a Dictionary with the contacts emails and customer uuid
    contacts_dict = dict(zip(df_conctacts['name'],df_conctacts['DB Email']))
    uuid_dict = dict(zip(df_conctacts['name'],df_conctacts['id']))
    toggle_dict = dict(zip(df_conctacts['name'],df_conctacts['Toggle']))
    toggle_dict_overdue = dict(zip(df_conctacts['id'],df_conctacts['Toggle']))
    
    # Mapping the emails to the dataframe
    df_gpd['contact_email'] = df_gpd['Customer_Name'].map(contacts_dict)
    df_gpd['Toggle'] = df_gpd['Customer_Name'].map(toggle_dict)
    df_gpd['pmt_ref_checks'] = df_gpd['Payment Ref'] + df_gpd['Customer_Name'].str.slice(stop=4)
    df_gpd['Original Pmt Amount'] = np.where(df_gpd['Payment Type']=='EFT', df_gpd['Payment Ref'].map(consolidated_logs_dict), np.where(df_gpd['Payment Type']=='Cash', df_gpd['Payment Ref'].map(consolidated_logs_dict), df_gpd['pmt_ref_checks'].map(checks_dict)))

    # Filtering out Toggle ON

    df_gpd = df_gpd.loc[df_gpd['Toggle']=='ON'].copy()
    df_gpd['Payment_Date'] = pd.to_datetime(df_gpd['Payment_Date'])
    df_gpd.sort_values(by=['Payment_Date'],inplace=True,ascending=False)
    df_gpd.rename(columns={'Amount':'Unapplied Amount'},inplace=True)
    df_gpd['Unapplied Amount'] = abs(df_gpd['Unapplied Amount'])

    # Creating final df using groupby
    customer_total_ua = df_gpd.groupby('Customer_Name').agg({'Unapplied Amount':'sum'}).reset_index()
    # List comprehension to get the data from each retailer
    details_account = [df_gpd.loc[df_gpd['Customer_Name']==x, ['Payment Ref','Payment_Date','Unapplied Amount','Original Pmt Amount','Issue Reason','Payment Type']].to_html(index=False, header=True ,justify='justify') for x in customer_total_ua['Customer_Name']]
    # Creating columns
    customer_total_ua['details'] = details_account
    customer_total_ua['Contact_email'] = customer_total_ua['Customer_Name'].map(contacts_dict)
    customer_total_ua['Unapplied Amount'] = customer_total_ua['Unapplied Amount'].round(2)
    customer_total_ua['AR_Rep'] = customer_total_ua['Customer_Name'].map(ar_rep_dict)
    customer_total_ua['uuid'] = customer_total_ua['Customer_Name'].map(uuid_dict)
    customer_total_ua['assign_id'] = customer_total_ua['AR_Rep'].map(front_teammate_id)
    
    

    # Loading the data into the Google sheets that Make will read
    update_gs_byID(st.secrets['gs_ID']['uncategorized'],customer_total_ua,sheet_name='UA_email_reachout_data',range_to_update='A1:N')

    data_aging_filter = data_aging[['Overdue','Delivery Date','Order Number','Due','Subtotal','Tax','Retailer UUID','Collected','Dispensary','Org Name']].copy()
    data_aging_filter.rename(columns={'Due':'Amount Due','Collected':'Total Collected','Org Name':'Brand'},inplace=True)
    #data_aging_filter = data_aging_filter.loc[data_aging_filter['Retailer UUID'].isin(customer_total_ua['uuid'])].copy()
    data_aging_filter['Total Invoice'] = (data_aging_filter['Subtotal'] + data_aging_filter['Tax']).round(2)
    data_aging_filter = data_aging_filter[['Overdue','Delivery Date','Order Number','Amount Due','Total Invoice','Retailer UUID','Total Collected','Dispensary','Brand']].copy()
    update_gs_byID(st.secrets['gs_ID']['uncategorized'],data_aging_filter,sheet_name='aging_nabis',range_to_update='A1:H')

    overdue_ar = data_aging_filter.loc[~data_aging_filter['Retailer UUID'].isin(customer_total_ua['uuid'])].copy()
    overdue_ar['Toggle'] = overdue_ar['Retailer UUID'].map(toggle_dict_overdue)
    overdue_ar = overdue_ar.loc[overdue_ar['Toggle'] == 'ON'].copy()
    overdue_ar_retailers = set(overdue_ar['Retailer UUID'])
    overdue_retailers_list = {'uuid':list(overdue_ar_retailers)}
    data_json = json.dumps(overdue_retailers_list)

    return data_json



def sameday_paperwork_data(data):
        
    update_gs_byID(st.secrets['gs_ID']['uncategorized'],data,sheet_name='1490_Uncategorized',range_to_update='A1:Q')

    # Reading Logs
    eft_data = read_gs_byID(st.secrets['gs_ID']['eft_log_ID'],eft_log_ws_id,'A:P')
    eft_df = pd.DataFrame(eft_data[0][1:],columns=eft_data[0][0])
    eft_df = eft_df[['Date','Transfer Amount','Payment Reference','Retailer']].copy()
    eft_df['Transfer Amount'] = eft_df['Transfer Amount'].replace('-',np.nan)
    eft_df['Transfer Amount'] = eft_df['Transfer Amount'].replace('',np.nan)
    eft_df.dropna(inplace=True)
    eft_df.rename(columns={'Transfer Amount':'Amount'},inplace=True)

    cash_data = read_gs_byID(st.secrets['gs_ID']['cash_log_ID'],cash_log_ws_id,'A:P')
    cash_df = pd.DataFrame(cash_data[0][1:],columns=cash_data[0][0])
    cash_df = cash_df[['Date','Amount','Payment Reference','Retailer']]
    cash_df['Amount'] = cash_df['Amount'].replace('-',np.nan)
    cash_df['Amount'] = cash_df['Amount'].replace('',np.nan)
    cash_df.dropna(inplace=True)

    checks_data = read_gs_byID(st.secrets['gs_ID']['check_log_ID'],checks_log_ws_id,'A:Q')
    checks_df = pd.DataFrame(checks_data[0][1:],columns=checks_data[0][0])
    checks_df = checks_df[['Date','Check Amount','Check Number','Retailer']]
    checks_df['Check Amount'] = checks_df['Check Amount'].replace('-',np.nan)
    checks_df['Check Amount'] = checks_df['Check Amount'].replace('',np.nan)
    checks_df.dropna(inplace=True)
    checks_df.rename(columns={'Check Amount':'Amount','Check Number':'Payment Reference'},inplace=True)

    logs_2022_data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],1559978804,'A:D')
    logs_2022_df = pd.DataFrame(logs_2022_data[0][1:],columns=logs_2022_data[0][0])

    consolidated_logs = pd.concat([logs_2022_df,eft_df,cash_df,checks_df])
    consolidated_logs_dict = dict(zip(consolidated_logs['Payment Reference'],consolidated_logs['Amount']))
    consolidated_logs['Retailer_ID'] = np.where(len(consolidated_logs['Retailer']) > 4, consolidated_logs['Payment Reference'] + consolidated_logs['Retailer'].str.slice(stop=4), consolidated_logs['Payment Reference'] + consolidated_logs['Retailer'])
    checks_dict = dict(zip(consolidated_logs['Retailer_ID'] ,consolidated_logs['Amount']))


    # Reading Uncategorized info from google sheets
    data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],ws_id,'A:Q')
    # Reading data into a pandas DF
    df = pd.DataFrame(data[0][1:],columns=data[0][0])
    df['Customer'] = df['Customer'].str.title()

    # Reading Uncategorized data from N2 to get the correct Payment Dates
    n2_data = read_gs_byID(st.secrets['gs_ID']['uncategorized'],n2_id,'A:P')
    # Reading data into a pandas DF
    df_n2 = pd.DataFrame(n2_data[0][1:],columns= n2_data[0][0])
    #Create Dictionary
    df_n2['pmt_ref'] = np.where(len(df_n2['Customer']) > 4, df_n2['Num'] + df_n2['Customer'].str.slice(stop=4), df_n2['Num'] + df_n2['Customer'])
    df_n2_dict = dict(zip(df_n2['pmt_ref'] , df_n2['Date']))
    # Create Column Payment Date
    df['pmt_ref_checks'] = df['Num'] + df['Customer'].str.slice(stop=4)
    df['Payment_Date'] = np.where(df['pmt_ref_checks'].isin(df_n2_dict) , df['Num'].map(df_n2_dict),df['Date'])

    # filtering Data
    reasons = ['UP:Order Overpaid',	'UP:Unidentified Overpayment',	'UP:Closed - Self Collected',	'UP:Closed - Previously Paid',	'UP:No Breakdown Provided',	'UP:Unclear Breakdown',	'UP:Incomplete Breakdown']
    df = df.loc[df['Class']=='OP - Unapplied'].copy()
    df = df.loc[df['Location'].isin(reasons)].copy()
    df['Create Date'] = df['Create Date'].apply(lambda x: pd.to_datetime(x)).dt.date
      
    df = df.loc[df['Create Date']== dt.date.today()].copy()
    # Cleaning data and converting to number
    df['Amount'] = df['Amount'].apply(lambda x: x.replace(',',''))
    df['Amount'] = pd.to_numeric(df['Amount'])
    df = df.loc[abs(df['Amount']) > 100 ].copy()
    
        
    # Creating new column payment type
    df['Payment Type'] = np.where(df['Num'].str.contains('Cash'),'Cash',np.where(df['Num'].str.contains('EFT'),'EFT','Check'))
    # Changing column names
    df.rename(columns={'Num':'Payment Ref','Location':'Issue Reason'},inplace=True)
    # Grouping the data 
    df_gpd = df.groupby(['Customer','Payment Ref','Payment_Date','Issue Reason','Payment Type']).agg({'Amount':'sum'}).reset_index()
    # Filtering out data with out info
    df_gpd = df_gpd.loc[df_gpd['Customer']!=''].copy()
    # Getting only the retailer name, removing Retailer Group:
    df_gpd['Customer_Name'] = df_gpd['Customer'].apply(lambda x: x.split('Group:')[-1].strip())


    # Reading gs file to get the AR associate
    ar_associate = read_gs_byID(st.secrets['gs_ID']['ar_associate_gs'],ws_ar_associate,'A:N')
    # Crreating Dataframe
    df_ar_rep = pd.DataFrame(ar_associate[0][1:],columns= ar_associate[0][0])
    df_ar_rep['Retailer'] = df_ar_rep['Retailer'].str.title()
    # Dictionary of AR emails contacts
    ar_rep_dict = dict(zip(df_ar_rep['Retailer'],df_ar_rep['Account Assignments']))



    # Reading in google sheets with contactas emails
    df_conctacts_data = read_gs_byID(st.secrets['gs_ID']['contacts_gs'],contacts_ws,'A:F')
    # Reading data into a pandas GS
    df_conctacts = pd.DataFrame(df_conctacts_data[0][1:],columns=df_conctacts_data[0][0])
    df_conctacts['name'] = df_conctacts['name'].str.title()
    # Creating a Dictionary with the contacts emails and customer uuid
    contacts_dict = dict(zip(df_conctacts['name'],df_conctacts['DB Email']))
    uuid_dict = dict(zip(df_conctacts['name'],df_conctacts['id']))
    toggle_dict = dict(zip(df_conctacts['name'],df_conctacts['Toggle']))
    # Mapping the emails to the dataframe
    df_gpd['contact_email'] = df_gpd['Customer_Name'].map(contacts_dict)
    df_gpd['Toggle'] = df_gpd['Customer_Name'].map(toggle_dict)
    df_gpd['pmt_ref_checks'] = df_gpd['Payment Ref'] + df_gpd['Customer_Name'].str.slice(stop=4)
    df_gpd['Original Pmt Amount'] = np.where(df_gpd['Payment Type']=='EFT', df_gpd['Payment Ref'].map(consolidated_logs_dict), np.where(df_gpd['Payment Type']=='Cash', df_gpd['Payment Ref'].map(consolidated_logs_dict), df_gpd['pmt_ref_checks'].map(checks_dict)))

    # Filtering out Toggle ON & 100 threshold
    
    # df_gpd = df_gpd.loc[df_gpd['Toggle']=='ON'].copy()
    df_gpd['Payment_Date'] = pd.to_datetime(df_gpd['Payment_Date'])
    df_gpd.sort_values(by=['Payment_Date'],inplace=True)
    df_gpd.rename(columns={'Amount':'Unapplied Amount'},inplace=True)
    df_gpd['Unapplied Amount'] = abs(df_gpd['Unapplied Amount'])

    # Creating final df using groupby
    customer_total_ua = df_gpd.groupby('Customer_Name').agg({'Unapplied Amount':'sum'}).reset_index()
    # List comprehension to get the data from each retailer
    details_account = [df_gpd.loc[df_gpd['Customer_Name']==x, ['Payment Ref','Payment_Date','Unapplied Amount','Original Pmt Amount','Issue Reason','Payment Type']].to_html(index=False, header=True) for x in customer_total_ua['Customer_Name']]
    # Creating columns
    customer_total_ua['details'] = details_account
    customer_total_ua['Contact_email'] = customer_total_ua['Customer_Name'].map(contacts_dict)
    customer_total_ua['Unapplied Amount'] = customer_total_ua['Unapplied Amount'].round(2)
    #customer_total_ua.rename(columns={'Amount':'Unapplied Amount'},inplace=True)
    customer_total_ua['AR_Rep'] = customer_total_ua['Customer_Name'].map(ar_rep_dict)
    customer_total_ua['uuid'] = customer_total_ua['Customer_Name'].map(uuid_dict)
    customer_total_ua['assign_id'] = customer_total_ua['AR_Rep'].map(front_teammate_id)
    customer_total_ua['Payment_reference'] = [df_gpd.loc[df_gpd['Customer_Name']==x, ['Payment Ref']] for x in customer_total_ua['Customer_Name']]
    

    # Loading the data into the Google sheets that Make will read
    update_gs_byID(st.secrets['gs_ID']['uncategorized'],customer_total_ua,sheet_name='UA_email_reachout_data',range_to_update='A1:N')




def get_dataframe_name(file):
    """
    Generates a name for the DataFrame based on the file name.
    """
    
    file_name = file.name.split(".")[0][:8]  # Get the file name without extension
    df_name = file_name.replace(" ", "_")  # Remove spaces and replace with underscores
    return df_name

def load_dataframe(file):
    """
    Loads the uploaded file into a Pandas DataFrame.
    """
    columns = ['Date',	'Transaction Type',	'Num',	'Name',	'Memo/Description',	'Account',	'Split',	'Amount',	'Balance',	'Created By',	'Last Modified By',	'Customer',	'A/R Paid',	'Class',	'Last Modified',	'Location',	'Create Date']
    file_extension = file.name.split(".")[-1]
    df_name = get_dataframe_name(file)

    if file_extension == "csv" and df_name == 'Nabifive':
        df = pd.read_csv(file,usecols=columns,skipfooter=1)
        
    elif file_extension == "xlsx" and df_name == 'Nabifive':
        df = pd.read_excel(file,usecols=columns,skipfooter=1)

    elif file_extension == "csv":
        df = pd.read_csv(file)

    elif file_extension == "xlsx":
        df = pd.read_excel(file)

    return df





if reports:

    # Create a dictionary to store dataframes
    dataframes = {}


    # Iterate through each uploaded file
    for file in reports:
        df_name = get_dataframe_name(file)
        df = load_dataframe(file)
        dataframes[df_name] = df

    data_uncategorized = dataframes['Nabifive']

    data_aging = dataframes['nabione-']
    
    data_uncategorized['Amount'] = pd.to_numeric(data_uncategorized['Amount'])
    GL_total = data_uncategorized['Amount'].sum().round(2)
    OP_unapplied = data_uncategorized.groupby('Class').agg({'Amount':'sum'}).reset_index()
    op_unaaplied_total = OP_unapplied[OP_unapplied['Class']=='OP - Unapplied']
    op_unaaplied_total_amt = op_unaaplied_total['Amount'].values
    op_unaaplied_total_amt = op_unaaplied_total_amt

    comparisson_total = (GL_total - op_unaaplied_total_amt).round(2)

    gl,op_unapplied = st.columns(2)

    with gl:
        st.metric('GL Total Balance',value=f'{GL_total:,.2f}')

    with op_unapplied:
        st.metric('OP Unapplied total', value=f'{op_unaaplied_total_amt[0]:,.2f}')

    

    if comparisson_total == 0.0:
        st.success(f'There is not variance on GL account, you can continue')
        st.write('What kind of emails are you sending?')
        consolidated = st.toggle('Consolidated')
        same_day = st.toggle('Same-day')
        if consolidated:
            if st.button('Consolidated weekly Emails'):
                data_json = paperwork_data(data_uncategorized,data_aging)               
                aging_webhook = 'https://hook.us1.make.com/spbz18uav6rjjqjcqchjiyg8gradoift'
                response = requests.post(aging_webhook)
                webhook = 'https://hook.us1.make.com/nlu4n0q2xvpbrr9fblw9mf4c4d7y8372'
                response = requests.post(webhook)
                overdue_webhook = 'https://hook.us1.make.com/sh7wnye6qqc8g6e2onzt0kgs9iz6n8t9'
                response = requests.post(overdue_webhook,data=data_json,headers={'Content-Type': 'application/json'})
            
                if response.status_code == 200:
                      st.success("Make Automation Running")
                else:
                      st.error(f"Failed to call webhook. Status Code: {response.status_code}")
            
        elif same_day:
            if st.button('Same-Day Emails'):
                sameday_paperwork_data(data_uncategorized)
                webhook_sameday = 'https://hook.us1.make.com/tsvqwj3idc30c317uodkze02y3mwf25d'
                response_sameday = requests.post(webhook_sameday)
        
                if response_sameday.status_code == 200:
                    st.success("Make Automation Running")
                else:
                    st.error(f"Failed to call webhook. Status Code: {response_sameday.status_code}")
    else:
        st.warning(f'There is a variance of {comparisson_total[0]:,.0f}, Please review the unapplied report.')


    



