
import sys
import requests
import json
import os
import psycopg2

## Numbers are returned in milliunits. 1000 milliunits equals one unit. 

ACCESS_TOKEN = os.getenv('YNAB_API_KEY')
print(ACCESS_TOKEN)
BASE_URL = 'https://api.youneedabudget.com/v1'

def connect_to_psql(host, user, password, database):
    try:
        # Establish a connection to the MariaDB server
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=database
        )
        return connection
    except psycopg2.Error as e:
        print(f"Error: {e}")
        sys.exit(1)

def get_budgets():
    endpoint = '/budgets'
    url = BASE_URL + endpoint

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
def get_transactions(budget_id):
    endpoint = f'/budgets/{budget_id}/transactions'
    url = BASE_URL + endpoint

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

def get_payee_list(budget_id):
    endpoint = f'/budgets/{budget_id}/payees'
    url = BASE_URL + endpoint

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
def get_accounts(budget_id):
    endpoint = f'/budgets/{budget_id}/accounts'
    url = BASE_URL + endpoint

    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
def drop_table(table_name, db_cursor):
    try:
        db_cursor.execute(f"""
        --sql
        DROP TABLE IF EXISTS {table_name}
        ;
        """)
    except Exception as e:
        print(f'Error occured while dropping the table {e}')
        
def create_tables(db_cursor):
  #Create tables if they dont exist
      
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS payees(
        id UUID primary key,
        payee_name TEXT NOT NULL
    )
    ; 
    """)
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS accounts(
        id UUID PRIMARY KEY, 
        account_name TEXT NOT NULL,
        account_type TEXT NOT NULL,
        balance INT NOT NULL,
        cleared_balance INT NOT NULL,
        uncleared_balance INT NOT NULL,
        transfer_payee_id UUID REFERENCES payees(id)
    )
    ;
    """)
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS category_groups(
        id UUID primary key,
        group_name TEXT NOT NULL
    )
    ;
    """)
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS categories(
        id UUID primary key,
        category_name TEXT NOT NULL,
        category_group_id UUID REFERENCES category_groups(id) NOT NULL
    )
    ;
    """)
    
    #Make sure to 
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS transactions(
        id UUID primary key,
        transaction_date DATE NOT NULL,
        amount INT NOT NULL,
        cleared TEXT NOT NULL,
        memo TEXT,
        payee_id UUID REFERENCES payees(id),
        account_id UUID REFERENCES accounts(id) NOT NULL,
        category_id UUID REFERENCES categories(id)
    ) 
    ;
    """)
    print("Tables created successfully")
    
def drop_table(db_cursor, db_name):
    db_cursor.execute(f"""
    --sql
    DROP TABLE IF EXISTS {db_name}
    ;
    """)
    
def insert_into_payees(db_cursor, payees_json):
    payees = []
    for payee in payees_json['data']['payees']:
        payees.append((payee['id'], payee['name']))
    insert_query = 'INSERT INTO payees(id, payee_name) VALUES (%s, %s)'
    db_cursor.executemany(insert_query, payees)
    print('Inserted Data into payees table')
    
def insert_into_accounts(db_cursor, accounts_json):
    accounts = []
    for account in accounts_json['data']['accounts']:
        accounts.append((account['id'], account['name'], account['type'], account['balance'], account['cleared_balance'], account['uncleared_balance'], account['transfer_payee_id']))
    insert_query = 'INSERT INTO accounts(id, account_name, account_type, balance, cleared_balance, uncleared_balance, transfer_payee_id) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    db_cursor.executemany(insert_query, accounts)
    print('Inserted into accounts table')
    
def main():
    #Connect to database
    with connect_to_psql('db', 'postgres', 'postgres', 'postgres') as connection:
        with connection.cursor() as cursor:
        
            #create_tables(cursor)
            
            #Order of input payee -> accounts -> category groups -> categories -> transactions
            
            #Get budget ID
            budget_id = get_budgets()['data']['budgets'][0]['id']
            
            #Get list of payees and insert
            payees_json = get_payee_list(budget_id)
            #insert_into_payees(cursor, payees_json)
            
            #Get list of accounts and insert
            accounts_json = get_accounts(budget_id)
            insert_into_accounts(cursor, accounts_json)
            
            
            
            #print(insert_into_payees(cursor, payees_json))
            #print(json.dumps(payees_json, indent=2))
            '''
            account_list = []
            for account in accounts['data']['accounts']:
                account_list.append(account['name'])
                
            transactions = get_transactions(budget_id)
            print(json.dumps(transactions, indent=2))
            '''
            
            cursor.executemany
            
            
            
            
            
            #Commit the cursor and close connection
            connection.commit()
            
    print("Succesfully disconnected from PostgreSQL")
      
    
main()