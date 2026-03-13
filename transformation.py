import pandas as pd

def run_transformation():
    data = pd.read_csv("zipco_transaction.csv")

    #Remove duplicates
    data.drop_duplicates(inplace=True)

    #handle missing values fill missing string/object values with 'unknown'
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data[col] = data[col].fillna('unknown')

    #handle missing values fill missing float/int  values with 'mean/median'
    float_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in float_columns:
        data[col] = data[col].fillna(data[col].mean())

    #Convert 'Date' column to datetime format
    data['Date'] = pd.to_datetime(data['Date'])

    #creating fact and dimension tables
    # create the product table
    products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'product_id'
    products = products.reset_index()

    #customers table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'customer_id'
    customers = customers.reset_index()

    #create staff table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'staff_id'
    staff = staff.reset_index()

    #create transaction table
    transactions = data.merge(products, on='ProductName', how='left') \
                    .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail'], how='left') \
                    .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')
    transactions.index.name = 'transaction_id'
    transactions = transactions.reset_index() \
                                [['Date','transaction_id', 'product_id', 'customer_id', 'staff_id', 'Quantity', 'UnitPrice', 'StoreLocation', \
                                'PaymentType', 'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', \
                                'DeliveryTime_min','OrderType',  'DayOfWeek','TotalSales']]

    # Save the transformed data to new CSV files
    data.to_csv('data/cleaned_data.csv', index=False)
    products.to_csv('data/products.csv', index=False)
    customers.to_csv('data/customers.csv', index=False)
    staff.to_csv('data/staff.csv', index=False)
    transactions.to_csv('data/transactions.csv', index=False)