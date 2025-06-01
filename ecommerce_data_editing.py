import pandas as pd


ecommerce = pd.read_excel('/Users/konradchrabaszcz/PycharmProjects/PythonCourses/analiza_danych_w_czasie_rzeczywistym_g/E-commerce.xlsx')

ecommerceSelected = ecommerce[['Ship Mode', 'Category', 'Sub-Category', 'Product Name', 'Sales', 'Quantity']].copy()
ecommerceSelected['Price'] = ecommerceSelected['Sales']/ecommerceSelected['Quantity']

ecommerceSelectedShipMode = pd.DataFrame(ecommerceSelected['Ship Mode'].drop_duplicates())
ecommerceSelectedShipMode.insert(1, 'Price', [3, 0, 7, 15])
ecommerceSelectedShipMode.reset_index(drop=True, inplace=True)
print(ecommerceSelectedShipMode)

ecommerceShipTypes = pd.DataFrame({'Ship Type': ['Courier', 'Parcel Machine', 'Pick-up Point'],
                                  'Price': [10, 0, 0]})
print(ecommerceShipTypes)

ecommerceSelectedProducts = ecommerceSelected.groupby(['Category',
                                                       'Sub-Category',
                                                       'Product Name']).agg({'Quantity': 'max',
                                                                             'Price': 'mean'}).reset_index()
ecommerceSelectedProducts['Warehouse Quantity'] = ecommerceSelectedProducts['Quantity']*100
ecommerceSelectedProducts['Price'] = round(ecommerceSelectedProducts['Price'], ndigits=2)
ecommerceSelectedProducts['Production Price'] = round(ecommerceSelectedProducts['Price']*0.75, ndigits=2)
ecommerceSelectedProducts.columns = ['Category', 'Sub-Category', 'Product Name', 'Max Order Quantity',
                                     'Price', 'Warehouse Quantity', 'Production Price']
ecommerceSelectedProducts.reset_index(drop=True, inplace=True)
print(ecommerceSelectedProducts)

ecommerceSelectedShipMode.to_csv('/Users/konradchrabaszcz/PycharmProjects/PythonCourses/analiza_danych_w_czasie_rzeczywistym_g/producer/data/shipModes.csv', index=False)
ecommerceSelectedProducts.to_csv('/Users/konradchrabaszcz/PycharmProjects/PythonCourses/analiza_danych_w_czasie_rzeczywistym_g/producer/data/products.csv', index=False)
ecommerceShipTypes.to_csv('/Users/konradchrabaszcz/PycharmProjects/PythonCourses/analiza_danych_w_czasie_rzeczywistym_g/producer/data/shipTypes.csv', index=False)