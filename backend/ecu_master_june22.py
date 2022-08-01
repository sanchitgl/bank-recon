import pandas as pd
import os
import sys
import re
import numpy as np
import recordlinkage

def reconcile(bank_book, bank_statement, previous_reco):
# input_path = 'C:\\Users\\amitu\\OneDrive\\quantuitix\\projects\\reconcify\\poc\\ecu\\bank_reco\\june_2022\\input_files\\'
# output_path = 'C:\\Users\\amitu\\OneDrive\\quantuitix\\projects\\reconcify\\poc\\ecu\\bank_reco\\june_2022\\output_files\\'

# bank_book = pd.read_excel(input_path + 'master_bankbook_june22.xlsx')

#-------------------------------------Process Bank Statements-----------------------------------

	bank_statement_deposits = pd.read_excel(bank_statement, sheet_name='Deposits And Credits')
	bank_statement_deposits = bank_statement_deposits[bank_statement_deposits['Ledger Date'] != 'Total']
	bank_statement_deposits['Ledger Date'] = pd.to_datetime(bank_statement_deposits['Ledger Date'], format='%m/%d/%y')
	bank_statement_deposits['TRN'] = bank_statement_deposits['Description'].str.extract('TRN: (.{12})')
	bank_statement_deposits['TRN'] = bank_statement_deposits['TRN'].fillna(bank_statement_deposits['Description'])
	bank_statement_deposits['Amount'] = bank_statement_deposits['Amount'].round(decimals=2)
	bank_statement_deposits = bank_statement_deposits.reset_index(drop=True)
	# print(bank_statement_deposits.info())
	# sys.exit()
	bank_statement_withdrawals = pd.read_excel(bank_statement, sheet_name='Withdrawals And Debits')
	bank_statement_withdrawals = bank_statement_withdrawals[bank_statement_withdrawals['Ledger Date'] != 'Total']
	bank_statement_withdrawals['Ledger Date'] = pd.to_datetime(bank_statement_withdrawals['Ledger Date'], format='%m/%d/%y')
	bank_statement_withdrawals['TRN'] = bank_statement_withdrawals['Description'].str.extract('TRN: (.{12})')
	bank_statement_withdrawals['TRN'] = bank_statement_withdrawals['TRN'].fillna(bank_statement_withdrawals['Description'])
	bank_statement_withdrawals['Amount'] = bank_statement_withdrawals['Amount'].round(decimals=2) * (-1)
	bank_statement_withdrawals = bank_statement_withdrawals.reset_index(drop=True)
	# print(bank_statement_withdrawals.info())
	# sys.exit()
	#------------------------------------One to one single amounts----------------------------------------
	bank_book = pd.read_excel(bank_book)
	bank_book_extract1 = bank_book[(bank_book['Journal number'] == '623814_008') | (bank_book['Journal number'] == '623856_008') | (bank_book['Journal number'] == '637750_008') | (bank_book['Journal number'] == '638435_008') | (bank_book['Journal number'] == '650265_008') | (bank_book['Journal number'] == '650449_008')]

	indexer1 = recordlinkage.Index()
	indexer1.block(left_on='Amount', right_on='Amount')
	comparisons1 = indexer1.index(bank_book_extract1, bank_statement_deposits)
	compare1 = recordlinkage.Compare()
	compare1.exact('Amount', 'Amount')
	result1 = compare1.compute(comparisons1, bank_book_extract1, bank_statement_deposits)
	result_reset1 = result1.reset_index()

	indexer2 = recordlinkage.Index()
	indexer2.block(left_on='Amount', right_on='Amount')
	comparisons2 = indexer2.index(bank_book_extract1, bank_statement_withdrawals)
	compare2 = recordlinkage.Compare()
	compare2.exact('Amount', 'Amount')
	result2 = compare2.compute(comparisons2, bank_book_extract1, bank_statement_withdrawals)
	result_reset2 = result2.reset_index()

	bank_book['Remarks'] = ''
	bank_book['TRN'] = ''
	bank_statement_deposits['Remarks'] = ''
	bank_statement_deposits['Journal number'] = ''
	bank_statement_withdrawals['Remarks'] = ''
	bank_statement_withdrawals['Journal number'] = ''

	for i in range(len(result_reset1)):
		bank_book['TRN'].iloc[result_reset1['level_0'].iloc[i]] = bank_statement_deposits['TRN'].iloc[result_reset1['level_1'].iloc[i]]
		bank_book['Remarks'].iloc[result_reset1['level_0'].iloc[i]] = 'One-to-one match'
		bank_statement_deposits['Journal number'].iloc[result_reset1['level_1'].iloc[i]] = bank_book['Journal number'].iloc[result_reset1['level_0'].iloc[i]]
		bank_statement_deposits['Remarks'].iloc[result_reset1['level_1'].iloc[i]] = 'One-to-one match'

	for i in range(len(result_reset2)):
		bank_book['TRN'].iloc[result_reset2['level_0'].iloc[i]] = bank_statement_withdrawals['TRN'].iloc[result_reset2['level_1'].iloc[i]]
		bank_book['Remarks'].iloc[result_reset2['level_0'].iloc[i]] = 'One-to-one match'
		bank_statement_withdrawals['Journal number'].iloc[result_reset2['level_1'].iloc[i]] = bank_book['Journal number'].iloc[result_reset2['level_0'].iloc[i]]
		bank_statement_withdrawals['Remarks'].iloc[result_reset2['level_1'].iloc[i]] = 'One-to-one match'

	# print(result_reset2)
	# sys.exit()
	bank_book.to_excel('bank_book.xlsx')
	bank_statement_deposits.to_excel('bank_statement_deposits.xlsx')
	bank_statement_withdrawals.to_excel('bank_statement_withdrawals.xlsx')

	# sys.exit()

	bank_book_grouped = bank_book[(bank_book['Journal number'] != '623814_008') & (bank_book['Journal number'] != '623856_008') & (bank_book['Journal number'] != '637750_008') & (bank_book['Journal number'] != '638435_008') & (bank_book['Journal number'] != '650265_008') & (bank_book['Journal number'] != '650449_008')].groupby(['Journal number', 'Date']).agg({'Amount': 'sum'}).reset_index()
	bank_book_grouped['Amount'] = bank_book_grouped['Amount'].round(decimals=2)

	#---------------------------------Match Date and Amount--------------------------------------
	indexer3 = recordlinkage.Index()
	indexer3.block(left_on='Amount', right_on='Amount')
	comparisons3 = indexer3.index(bank_book_grouped, bank_statement_deposits)
	compare3 = recordlinkage.Compare()
	compare3.exact('Amount', 'Amount')
	result3 = compare3.compute(comparisons3, bank_book_grouped, bank_statement_deposits)
	result_reset3 = result3.reset_index()
	# print(result_reset1)

	bank_book_grouped['Remarks'] = ''

	#----------------------------------------Exract Unique---------------------------------------------------

	result_reset3 = result_reset3.merge(bank_book_grouped[['Journal number']], left_on='level_0', right_index=True)
	result_reset3 = result_reset3.merge(bank_statement_deposits[['TRN']], left_on='level_1', right_index=True)
	result_reset3 = result_reset3.merge(bank_statement_deposits[['Ledger Date']], left_on='level_1', right_index=True)
	result_reset3['TRN'] = result_reset3['TRN'] + ' ' + result_reset3['Ledger Date'].dt.strftime('%m/%d/%y')
	# print(result_reset3['TRN'])
	# sys.exit()

	bank_book_grouped = bank_book_grouped.merge(result_reset3[['level_0', 'TRN']].rename(columns={'TRN': 'TRN2'}), left_index=True, right_on='level_0', how='outer').set_index('level_0')
	bank_statement_deposits = bank_statement_deposits.merge(result_reset3[['level_1', 'Journal number']].rename(columns={'Journal number': 'Journal number2'}), left_index=True, right_on='level_1', how='outer').set_index('level_1')
	bank_book = bank_book.merge(bank_book_grouped[['Journal number', 'TRN2']], on='Journal number', how='left')
		
	# bank_book_grouped.to_excel(output_path + 'master_bankbook_grouped_june22_reconciled.xlsx')
	writer = pd.ExcelWriter('temp/master_reconciled.xlsx', engine='xlsxwriter')
	bank_statement_deposits.to_excel(writer,sheet_name='deposits')
	bank_statement_withdrawals.to_excel(writer,sheet_name='withdrawals')
	bank_book.to_excel(writer,sheet_name='bank_book')
	writer.save()

	return