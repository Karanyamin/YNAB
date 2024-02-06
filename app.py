
import sys
import requests
import json
import os
import psycopg2

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
    
    
def main():
    #Connect to database
    connection = connect_to_psql('db', 'postgres', 'postgres', 'postgres')
    cursor = connection.cursor()
    
    
    #Create tables if they dont exist
    cursor.execute("""
    --sql
    CREATE TABLE IF NOT EXISTS accounts (
        id serial primary key,
        name varchar(255),
        
    )
    ;
    """)
    
    
    budgets = get_budgets()
    #print(json.dumps(budgets, indent=2))
    budget_id = budgets['data']['budgets'][0]['id']
    accounts = get_accounts(budget_id)
    #print(json.dumps(accounts, indent=2))
    account_list = []
    for account in accounts['data']['accounts']:
        account_list.append(account['name'])
        
    transactions = get_transactions(budget_id)
    print(json.dumps(transactions, indent=2))
    
    
    
    
    
    
    
    
    #Commit the cursor and close connection
    connection.commit()
    cursor.close()
    connection.close()
    print("Succesfully disconnected from MariaDB")
      
    
main()