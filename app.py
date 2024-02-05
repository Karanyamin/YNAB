
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
            database=database
        )

        try:
            # Create a cursor
            cursor = connection.cursor()

            # Execute a lightweight query (e.g., SELECT 1)
            cursor.execute("SELECT 1;")

            # Fetch the result (optional)
            result = cursor.fetchone()

            # Close the cursor
            cursor.close()
            print("Connected to MariaDB")
            return connection
        except:
            print('Error')
            return 

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return
'''
def connect_to_mariadb(host, user, password, database):
    try:
        # Establish a connection to the MariaDB server
        connection = mariadb.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        try:
            connection.ping()
            print("Connected to MariaDB")
            return connection
        except:
            return 

    except mariadb.Error as e:
        print(f"Error: {e}")
        return
'''

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
    
def close_to_mariadb(connection):
    if connection:
        connection.close()
        print("Succesfully disconnected from MariaDB")
    else:
        print("Could not disconnect from MariaDB")
    
def main():
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
    
    # Connect to server
    connection = connect_to_psql('db', 'postgres', 'postgres', 'postgres')
    
    
    
    
    
    
    
    # Close the connection
    close_to_mariadb(connection)    
    
main()