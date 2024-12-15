import kaggle
import pandas as pd
import sqlite3
import uuid

# Download the dataset
kaggle.api.authenticate()
kaggle.api.dataset_download_files('aungpyaeap/supermarket-sales',path='data',unzip=True)

# Renaming columns

rename_dict = {'Invoice ID': 'invoice_id', 
                'Branch': 'branch',
                'City': 'city',
                'Customer type': 'customer_type',
                'Gender': 'gender',
                'Product line': 'product_line',
                'Unit price': 'unit_price',
                'Quantity': 'quantity',
                'Tax 5%': 'tax',
                'Total': 'total',
                'Date': 't_date',
                'Time': 't_time',
                'Payment': 'payment',
                'cogs': 'cogs',
                'gross margin percentage': 'gross_margin_percentage',
                'gross income': 'gross_income',
                'Rating': 'rating'}
                

# Load the dataset
data_df = pd.read_csv('data\supermarket_sales - Sheet1.csv')
data_df.rename(columns=rename_dict, inplace=True)

# Create Product Dimension Table with UUIDs
product_dim = data_df[['product_line', 'unit_price', 'gross_margin_percentage']].drop_duplicates()
product_dim['product_id'] = [str(uuid.uuid4()) for _ in range(len(product_dim))]  
product_dim = product_dim[['product_id', 'product_line', 'unit_price', 'gross_margin_percentage']]

# Create Branch Dimension Table with UUIDs
branch_dim = data_df[['branch', 'city']].drop_duplicates()
branch_dim['branch_id'] = [str(uuid.uuid4()) for _ in range(len(branch_dim))] 
branch_dim = branch_dim[['branch_id', 'branch', 'city']]

# Create Fact Table and use UUIDs for Product_ID and Branch_ID
data_df['branch_id'] = pd.merge(data_df[['branch','city']],branch_dim,on=['branch','city'],how='left')['branch_id']
data_df['product_id'] = pd.merge(data_df[['product_line', 'unit_price', 'gross_margin_percentage']],product_dim,on=['product_line', 'unit_price', 'gross_margin_percentage'],how='left')['product_id']

fact_table = data_df[['invoice_id', 'branch_id','customer_type','gender', 'product_id','quantity', 'tax', 'total', 't_date', 't_time', 'payment', 'cogs', 'gross_income', 'rating']]

# Cursor connection to the DB
conn = sqlite3.connect('data/sales_data.db')
cursor = conn.cursor()

# Load data into the tables
product_dim.to_sql('product', conn, if_exists='replace', index=False)
branch_dim.to_sql('branch', conn, if_exists='replace', index=False)
fact_table.to_sql('sales', conn, if_exists='replace', index=False)

# Commit and close the connection
conn.commit()
conn.close()