import pandas as pd
import os
import sys
import re
import numpy as np
import recordlinkage

def reconcile(bank_book, bank_statement, previous_reco):
# input_path = 'C:\\Users\\amitu\\OneDrive\\quantuitix\\projects\\reconcify\\poc\\ecu\\bank_reco\\june_2022\\input_files\\'
# output_path = 'C:\\Users\\amitu\\OneDrive\\quantuitix\\projects\\reconcify\\poc\\ecu\\bank_reco\\june_2022\\output_files\\'

	bank_book = pd.read_excel(bank_book)
# bank_book = bank_book[bank_book['Date'] != 'Total']
# bank_book = bank_book[bank_book['Amount'] > 0]
	bank_book_grouped = bank_book.groupby(['Journal number', 'Date']).agg({'Amount': 'sum'}).reset_index()
	bank_book_grouped['Amount'] = bank_book_grouped['Amount'].round(decimals=2)
	# print(bank_book_grouped.info())

	#-------------------------------------Process Bank Statements-----------------------------------

	bank_statement = pd.read_excel(bank_statement, sheet_name='Deposits And Credits')
	bank_statement = bank_statement[bank_statement['Ledger Date'] != 'Total']
	bank_statement['Ledger Date'] = pd.to_datetime(bank_statement['Ledger Date'], format='%m/%d/%y')
	bank_statement['TRN'] = bank_statement['Description'].str.extract('TRN: (.{12})')
	bank_statement['TRN'] = bank_statement['TRN'].fillna(bank_statement['Description'])
	bank_statement['Amount'] = bank_statement['Amount'].round(decimals=2)
	bank_statement = bank_statement.reset_index(drop=True)
	# print(bank_statement.info())

	#---------------------------------Match Date and Amount--------------------------------------
	indexer1 = recordlinkage.Index()
	indexer1.block(left_on=['Amount'], right_on=['Amount'])
	comparisons1 = indexer1.index(bank_book_grouped, bank_statement)
	compare1 = recordlinkage.Compare()
	compare1.exact('Amount', 'Amount')
	result1 = compare1.compute(comparisons1, bank_book_grouped, bank_statement)
	result_reset1 = result1.reset_index()
	# print(result_reset1)

	bank_book_grouped['Remarks'] = ''
	bank_statement['Remarks'] = ''

	#----------------------------------------Exract Unique---------------------------------------------------

	if len(result_reset1) > 0:
		unique = result_reset1[~result_reset1.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			bank_book_grouped['Remarks'].iloc[unique['level_0'].to_list()] = 'Unique Match'
			bank_statement['Remarks'].iloc[unique['level_1'].to_list()] = 'Unique Match'
			unique = pd.merge(unique, bank_book_grouped[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement[['TRN']], left_on='level_1', right_index=True)

			bank_book_grouped = pd.merge(bank_book_grouped, unique[['level_0', 'TRN']], left_index=True, right_on='level_0', how='outer').set_index('level_0')
			bank_statement = pd.merge(bank_statement, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').set_index('level_1')
			
			# print(bank_book_grouped.info())
			# print(bank_statement.info())

		bank_book_grouped2 = bank_book_grouped[bank_book_grouped['Remarks'] == ''].drop(['TRN'], axis=1)
		bank_statement2 = bank_statement[bank_statement['Remarks'] == ''].drop(['Journal number'], axis=1)

		indexer2 = recordlinkage.Index()
		indexer2.block(left_on='Amount', right_on='Amount')
		comparisons2 = indexer2.index(bank_book_grouped2, bank_statement2)
		compare2 = recordlinkage.Compare()
		compare2.exact('Amount', 'Amount')
		result2 = compare2.compute(comparisons2, bank_book_grouped2, bank_statement2)
		result_reset2 = result2.reset_index()
		# print(result_reset2)
		
		if len(result_reset2) > 0:
			result_reset2 = pd.merge(result_reset2, bank_book_grouped[['Date', 'Journal number', 'Amount']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount': 'Amount Book'})
			result_reset2 = pd.merge(result_reset2, bank_statement[['Ledger Date', 'TRN', 'Amount']], left_on='level_1', right_index=True).rename(columns={'Ledger Date': 'Date Statement', 'Amount': 'Amount Statement'})
			result_reset2['Days Difference'] = abs(result_reset2['Date Book'] - result_reset2['Date Statement'])

			list_a = []
			for a in list(set(result_reset2['Amount Book'].to_list())):
				df = result_reset2[result_reset2['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				while len(result_reset2) > 0:
					bank_book_grouped['Remarks'].iloc[result_reset2['level_0'].iloc[0]] = 'Duplicate Match'
					bank_book_grouped['TRN'].iloc[result_reset2['level_0'].iloc[0]] = result_reset2['TRN'].iloc[0]
					bank_statement['Remarks'].iloc[result_reset2['level_1'].iloc[0]] = 'Duplicate Match'
					bank_statement['Journal number'].iloc[result_reset2['level_1'].iloc[0]] = result_reset2['Journal number'].iloc[0]

					result_reset2 = result_reset2[(result_reset2['level_0'] != result_reset2['level_0'].iloc[0]) & (result_reset2['level_1'] != result_reset2['level_1'].iloc[0])]
					# print(result_reset2)
		
		#----------------------------Handling duplicate journal numbers in bank_book_grouped----------------------
		bank_book_grouped_grouped = bank_book_grouped.groupby(['Journal number', 'Remarks', 'TRN']).agg({'Date': 'count'}).reset_index()
		bank_book = pd.merge(bank_book, bank_book_grouped_grouped[['Journal number', 'Remarks', 'TRN']], on='Journal number', how='left')
		
	# bank_book_grouped.to_excel(output_path + 'ar_bankbook_grouped_june22_reconciled.xlsx')
	writer = pd.ExcelWriter('temp/ar_bankstatement_bankbook_reconciled.xlsx', engine='xlsxwriter')
	bank_statement.to_excel(writer,sheet_name='bankstatement')
	bank_book.to_excel(writer,sheet_name='bankbook')
	writer.save()

	return