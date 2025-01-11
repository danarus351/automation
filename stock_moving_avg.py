import yfinance as yf
import statsmodels.api as sm
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd


# Fetch stock data
stock_symbol = 'AAPL'
stock_data = yf.download(stock_symbol, start='2023-01-01', end='2023-08-01')

# Calculate descriptive statistics
summary_stats = stock_data['Close'].describe()
print(summary_stats)

# Prepare the data
stock_data['Date'] = stock_data.index
stock_data['Date_ordinal'] = pd.to_datetime(stock_data['Date']).map(pd.Timestamp.toordinal)

# Define the dependent and independent variables
X = stock_data['Date_ordinal']
y = stock_data['Close']

# Add a constant to the independent variable (required by statsmodels)
X = sm.add_constant(X)

# Fit the linear regression model
model = sm.OLS(y, X).fit()

# Print the model summary
print(model.summary())

# Plot the results
plt.figure(figsize=(10, 5))
plt.plot(stock_data['Date'], stock_data['Close'], label='Closing Price')
plt.plot(stock_data['Date'], model.predict(X), label='Fitted Line', linestyle='--')
plt.title(f'{stock_symbol} Stock Closing Prices with Linear Regression')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()
plt.show()
