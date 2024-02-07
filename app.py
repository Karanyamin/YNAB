
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
    CREATE TABLE IF NOT EXISTS accounts(
        id serial primary key,
        ynab_id varchar(255),
        account_name varchar(255),
        account_type varchar(255),
        balance INT,
        cleared_balance INT,
        uncleared_balance INT
    )
    ;
    """)  
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS payees(
        id serial primary key,
        ynab_id varchar(255),
        payee_name varchar(255)
    )
    ; 
    """)
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS category_groups(
        id serial primary key,
        group_name varchar(255)
    )
    ;
    """)
    
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS categories(
        id serial primary key,
        category_name varchar(255),
        category_group_id INT REFERENCES category_groups(id)
    )
    ;
    """)
    
    #Make sure to 
    db_cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS transactions(
        id serial primary key,
        transaction_date DATE,
        amount INT,
        cleared BOOLEAN,
        memo varchar(255),
        payee_id INT REFERENCES payees(id),
        account_id INT REFERENCES accounts(id),
        category_id INT REFERENCES categories(id)
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
    

    
    
def main():
    #Connect to database
    connection = connect_to_psql('db', 'postgres', 'postgres', 'postgres')
    cursor = connection.cursor()
    
    create_tables(cursor)
    
    budgets = get_budgets()
    #print(json.dumps(budgets, indent=2))
    budget_id = budgets['data']['budgets'][0]['id']
    transactions = get_transactions(budget_id)
    #print(json.dumps(transactions, indent=2))
    '''
    account_list = []
    for account in accounts['data']['accounts']:
        account_list.append(account['name'])
        
    transactions = get_transactions(budget_id)
    print(json.dumps(transactions, indent=2))
    '''
    
    
    
    
    
    
    
    #Commit the cursor and close connection
    connection.commit()
    cursor.close()
    connection.close()
    print("Succesfully disconnected from MariaDB")
      
    
main()