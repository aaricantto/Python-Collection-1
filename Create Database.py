import sqlite3
import pandas as pd

import yfinance as yf
from yahoo_fin.stock_info import get_balance_sheet



class stock_db_manager:
	# Establish connection to sql database
	def __init__(self, index_name):
		self.index_name = index_name
		self.connection = sqlite3.connect(self.index_name+'.db')
		self.cursor = self.connection.cursor()

	### -----------------------------------------------------------
	# webscrape ticker symbols of index from wiki and create key
	def create_tickers(self, url):
		self.url = 'https://en.wikipedia.org/wiki/' + url
		scrape = pd.read_html(self.url)[1]
		self.tickers = scrape[scrape.columns[1]].dropna()
		self.tickers.to_sql('tickers', self.connection, if_exists = 'replace')

	# Use tickers to download create SQL tables of price data for each time interval
	def create_technical_db(self, intervals):
		for interval in intervals:
			data = []
			for ticker in self.tickers:
				print(ticker)
				frame = yf.download(ticker, interval = interval).reset_index()				
				frame['ticker'] = ticker
				data.append(frame)

			price_data = pd.concat(data, axis = 0)
			name = interval+'_'+'price_data'
			price_data.to_sql(name, self.connection, if_exists = 'replace')

	# Use tickers to download create SQL tables of fundamental data
	def create_fundamental_db(self):
		data = []
		for ticker in self.tickers:
			print(ticker)
			frame = get_balance_sheet(ticker).T.reset_index()
			frame['ticker'] = ticker
			data.append(frame)

		fundamental_data = pd.concat(data, axis = 0)
		fundamental_data.to_sql('fundamentals', self.connection, if_exists = 'replace')

	### -----------------------------------------------------------
	# Load Data from database
	def load_tickers(self):
		self.cursor.execute("SELECT * FROM tickers")
		self.tickers = pd.DataFrame(self.cursor.fetchall())[[1]]
		self.tickers.columns = ['tickers']
		return(self.tickers)

	def load_technicals(self, interval):
		name = "'"+interval+'_'+"price_data'"
		data = self.cursor.execute("SELECT * FROM " + name)
		
		self.technical = pd.DataFrame(self.cursor.fetchall()).dropna()
		self.technical.columns = [column[0] for column in data.description]
		return(self.technical.drop(['index'], axis = 1)) 

	def load_fundamantals(self):
		data = self.cursor.execute("SELECT * FROM 'fundamentals'")
		self.fundamental = pd.DataFrame(self.cursor.fetchall())
		self.fundamental.columns = [column[0] for column in data.description]
		return(self.fundamental.drop(['index'], axis = 1))

	def exct(self, phrase):
		self.cursor.execute(phrase)

	def delete_row(self, table, id):
	    """
	    Delete a task by task id
	    :param conn:  Connection to the SQLite database
	    :param id: id of the task
	    :return:
	    """
	    sql = 'DELETE FROM '+table+ ' WHERE ticker = ' + id
	    cur = self.connection.cursor()
	    print(sql)

	    cur.execute(sql)
	    self.connection.commit()

# Create database for index  -------------------------------------------
# 
db_manager = stock_db_manager('S&P 500')

# db_manager.create_tickers('List_of_S%26P_500_companies')
# print(db_manager.tickers)
# # Create tables in database for price data with varying times

# db_manager.create_technical_db(["1d", "5d", "1mo"])

# db_manager.create_fundamental_db()


# ----------------------------------------------------------------------


# Load database for index  -------------------------------------------

tickers = db_manager.load_tickers()

technical_data = db_manager.load_technicals('1mo')
fundamental_data = db_manager.load_fundamantals().dropna().drop('level_0', axis =1)


print(tickers)
print(technical_data)
print(fundamental_data)


# Clean up each table -------------------------------------------
# def clean():

	# Removed fundamental stats:
	#shortTermInvestments
	#deferredLongTermLiab
	# minorityInterest
	#shortLongTermDebt
	#deferredLongTermAssetCharges
	# longTermInvestments
	# inventory
	# goodWill
	# otherCurrentAssets
	# intangibleAssets
	# capitalSurplus
	# longTermDebt
	# commonStock
	# retainedEarnings


	# remove rows with NA
	# otherCurrentLiab
	# otherStockholderEquity

	# nas = fundamental_data.isna()['accountsPayable']
	# ids = [i for i, x in enumerate(nas) if x]
	# print(len(ids), ids)
	# for ide in ids:
	# 	db_manager.delete_row('fundamentals', ide)


print('hi')

nas = (fundamental_data['ticker'].value_counts() >=3)
nas = nas.index[nas == True]
iss = list()
for i in range(len(fundamental_data)):
	if nas[nas.isin([fundamental_data['ticker'].iloc[i]])].empty == True:
		print(nas[nas.isin([fundamental_data['ticker'].iloc[i]])].empty)
		print(i)
		iss.append(i)


fundamental_data = fundamental_data.drop(fundamental_data.index[iss]).reset_index(drop = True)

tickers = (list(set(fundamental_data['ticker'])))

