import pandas as pd
import os
import sys
import re
import numpy as np
import recordlinkage

def reconcile(bank_book, bank_statement, previous_reco):

	bank_book = pd.read_excel(bank_book, skiprows=4)
	bank_book.dropna(subset=['Amount FC'], inplace=True)
	bank_book = bank_book[(bank_book['Amount FC'] != 'Amount FC')]

	bank_book_grouped = bank_book.groupby(['Journal number', 'Date']).agg({'Amount FC': 'sum'}).reset_index()
	bank_book_grouped['Amount FC'] = bank_book_grouped['Amount FC'].round(decimals=2)

	#-------------------------------------Process Bank Statements-----------------------------------

	bank_statement = pd.read_excel(bank_statement, sheet_name='Details')
	bank_statement = bank_statement[bank_statement['Transaction Date'] != 'Total']
	bank_statement['Transaction Date'] = pd.to_datetime(bank_statement['Transaction Date'], format='%m/%d/%y')
	bank_statement['Credit Amount'] = bank_statement['Credit Amount'].round(decimals=2)
	bank_statement['Debit Amount'] = bank_statement['Debit Amount'].round(decimals=2)

	bank_statement.fillna('', inplace=True)
	bank_statement_deposits = bank_statement[bank_statement['Credit Amount'] != '']
	bank_statement_deposits['Credit Amount'] = bank_statement_deposits['Credit Amount'].astype(float)
	bank_statement_withdrawals = bank_statement[bank_statement['Debit Amount'] != '']
	bank_statement_withdrawals['Debit Amount'] = bank_statement_withdrawals['Debit Amount'].astype(float)

	#---------------------------------Match Date and Amount--------------------------------------
	indexer1 = recordlinkage.Index()
	indexer1.block(left_on=['Amount FC'], right_on=['Credit Amount'])
	comparisons1 = indexer1.index(bank_book_grouped, bank_statement_deposits)
	compare1 = recordlinkage.Compare()
	compare1.exact('Amount FC', 'Credit Amount')
	result1 = compare1.compute(comparisons1, bank_book_grouped, bank_statement_deposits)
	result_reset1 = result1.reset_index()

	#----------------------------------------Exract Unique---------------------------------------------------

	if len(result_reset1) > 0:
		unique = result_reset1[~result_reset1.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book_grouped[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_deposits[['Bank Reference']], left_on='level_1', right_index=True)

			bank_book_grouped = pd.merge(bank_book_grouped, unique[['level_0', 'Bank Reference']], left_index=True, right_on='level_0', how='outer').rename(columns={'Bank Reference': 'Bank Reference 1'}).set_index('level_0')
			bank_statement_deposits = pd.merge(bank_statement_deposits, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').rename(columns={'Journal number': 'Journal number 1'}).set_index('level_1')
			
		bank_book_grouped['Bank Reference 1'].fillna('', inplace=True)
		bank_statement_deposits['Journal number 1'].fillna('', inplace=True)
		bank_book_grouped2 = bank_book_grouped[bank_book_grouped['Bank Reference 1'] == ''].drop(['Bank Reference 1'], axis=1)
		bank_statement_deposits2 = bank_statement_deposits[bank_statement_deposits['Journal number 1'] == ''].drop(['Journal number 1'], axis=1)

		indexer2 = recordlinkage.Index()
		indexer2.block(left_on='Amount FC', right_on='Credit Amount')
		comparisons2 = indexer2.index(bank_book_grouped2, bank_statement_deposits2)
		compare2 = recordlinkage.Compare()
		compare2.exact('Amount FC', 'Credit Amount')
		result2 = compare2.compute(comparisons2, bank_book_grouped2, bank_statement_deposits2)
		result_reset2 = result2.reset_index()
		
		if len(result_reset2) > 0:
			result_reset2 = pd.merge(result_reset2, bank_book_grouped[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			result_reset2 = pd.merge(result_reset2, bank_statement_deposits[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			result_reset2['Days Difference'] = abs(result_reset2['Date Book'] - result_reset2['Date Statement'])

			for a in list(set(result_reset2['Amount Book'].to_list())):
				df = result_reset2[result_reset2['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				while len(df) > 0:
					bank_book_grouped.at[df['level_0'].iloc[0], 'Bank Reference 1'] = df['Bank Reference'].iloc[0]
					bank_statement_deposits.at[df['level_1'].iloc[0], 'Journal number 1'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

		#----------------------------Handling duplicate journal numbers in bank_book_grouped----------------------
		bank_book_grouped_grouped = bank_book_grouped.groupby(['Journal number', 'Bank Reference 1']).agg({'Date': 'count'}).reset_index()
		bank_book = pd.merge(bank_book, bank_book_grouped_grouped[['Journal number', 'Bank Reference 1']], on='Journal number', how='left')

	else:
		bank_book_grouped['Bank Reference 1'] = ''
		bank_statement_deposits['Journal number 1'] = ''
		
	#-----------------------------Withdrawals-------------------------------------------
	bank_statement_withdrawals['Debit Amount'] = -bank_statement_withdrawals['Debit Amount']
	bank_book_withdrawals = bank_book[bank_book['Amount FC'] < 0 ]
	bank_book_withdrawals['Bank Reference 1'].fillna('', inplace=True)
	bank_book_withdrawals = bank_book_withdrawals[bank_book_withdrawals['Bank Reference 1'] == '']

	indexer3 = recordlinkage.Index()
	indexer3.block(left_on=['Amount FC'], right_on=['Debit Amount'])
	comparisons3 = indexer3.index(bank_book_withdrawals, bank_statement_withdrawals)
	compare3 = recordlinkage.Compare()
	compare3.exact('Amount FC', 'Debit Amount')
	result3 = compare3.compute(comparisons3, bank_book_withdrawals, bank_statement_withdrawals)
	result_reset3 = result3.reset_index()

	#----------------------------------------Exract Unique---------------------------------------------------

	if len(result_reset3) > 0:
		unique = result_reset3[~result_reset3.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book_withdrawals[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_withdrawals[['Bank Reference']], left_on='level_1', right_index=True)

			bank_book_withdrawals = pd.merge(bank_book_withdrawals, unique[['level_0', 'Bank Reference']], left_index=True, right_on='level_0', how='outer').rename(columns={'Bank Reference': 'Bank Reference 2'}).set_index('level_0')
			bank_statement_withdrawals = pd.merge(bank_statement_withdrawals, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').rename(columns={'Journal number': 'Journal number 2'}).set_index('level_1')

		bank_book_withdrawals2 = bank_book_withdrawals[bank_book_withdrawals['Bank Reference 2'] == ''].drop(['Bank Reference 2'], axis=1)
		bank_statement_withdrawals2 = bank_statement_withdrawals[bank_statement_withdrawals['Journal number 2'] == ''].drop(['Journal number 2'], axis=1)
		bank_book_withdrawals2['Amount FC'] = bank_book_withdrawals2['Amount FC'].astype(float)

		indexer4 = recordlinkage.Index()
		indexer4.block(left_on='Amount FC', right_on='Debit Amount')
		comparisons4 = indexer4.index(bank_book_withdrawals2, bank_statement_withdrawals2)
		compare4 = recordlinkage.Compare()
		compare4.exact('Amount FC', 'Debit Amount')
		result4 = compare4.compute(comparisons4, bank_book_withdrawals2, bank_statement_withdrawals2)
		result_reset4 = result4.reset_index()
		
		if len(result_reset4) > 0:
			resut_reset4 = pd.merge(resut_reset4, bank_book_withdrawals[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			resut_reset4 = pd.merge(resut_reset4, bank_statement_withdrawals[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			resut_reset4['Days Difference'] = abs(resut_reset4['Date Book'] - resut_reset4['Date Statement'])

			for a in list(set(result_reset4['Amount Book'].to_list())):
				df = result_reset4[result_reset4['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				while len(df) > 0:
					bank_book_withdrawals.at[df['level_0'].iloc[0], 'Bank Reference 2'] = df['Bank Reference'].iloc[0]
					bank_statement_withdrawals.at[df['level_1'].iloc[0], 'Journal number 2'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]
	else:
		bank_book_withdrawals['Bank Reference 2'] = ''
		bank_statement_withdrawals['Journal number 2'] = ''

	bank_book = bank_book.merge(bank_book_withdrawals[['Bank Reference 2']], left_index=True, right_index=True, how='left')
	bank_book['Bank Reference 1'].fillna('', inplace=True)
	bank_book['Bank Reference 2'].fillna('', inplace=True)
	bank_book['Bank Reference'] = np.where((bank_book['Bank Reference 1'] != '') & (bank_book['Bank Reference 2'] == ''), bank_book['Bank Reference 1'], np.where((bank_book['Bank Reference 1'] == '') & (bank_book['Bank Reference 2'] != ''), bank_book['Bank Reference 2'], ''))
	bank_book.drop(['Bank Reference 1', 'Bank Reference 2'], axis=1, inplace=True)

	bank_statement = bank_statement.merge(bank_statement_deposits[['Journal number 1']], left_index=True, right_index=True, how='left')
	bank_statement = bank_statement.merge(bank_statement_withdrawals[['Journal number 2']], left_index=True, right_index=True, how='left')
	bank_statement['Journal number 1'].fillna('', inplace=True)
	bank_statement['Journal number 2'].fillna('', inplace=True)
	bank_statement['Journal number'] = np.where((bank_statement['Journal number 1'] != '') & (bank_statement['Journal number 2'] == ''), bank_statement['Journal number 1'], np.where((bank_statement['Journal number 1'] == '') & (bank_statement['Journal number 2'] != ''), bank_statement['Journal number 2'], ''))
	bank_statement.drop(['Journal number 1', 'Journal number 2'], axis=1, inplace=True)

	bank_book['Date'] = pd.to_datetime(bank_book['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
	bank_statement['Transaction Date'] = bank_statement['Transaction Date'].dt.strftime('%Y-%m-%d')
	bank_statement['Value Date'] = bank_statement['Transaction Date']

	writer = pd.ExcelWriter('temp/ar_reconciled.xlsx', engine='xlsxwriter')
	bank_book.to_excel(writer, sheet_name='Bank Book', index=False)
	bank_statement.to_excel(writer, sheet_name='Bank Statement', index=False)
		
	workbook = writer.book
	fail_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
	pass_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})

	sheet1 = writer.sheets['Bank Book']
	sheet2 = writer.sheets['Bank Statement']

	sheet1.conditional_format('V2:V'+str(len(bank_book)+1), {'type': 'blanks', 'format': fail_format})
	sheet1.conditional_format('V2:V'+str(len(bank_book)+1), {'type': 'no_blanks', 'format': pass_format})
	sheet2.conditional_format('AI2:AI'+str(len(bank_statement)+1), {'type': 'blanks', 'format': fail_format})
	sheet2.conditional_format('AI2:AI'+str(len(bank_statement)+1), {'type': 'no_blanks', 'format': pass_format})

	sheet1.set_column('A:V', 20, None)
	sheet2.set_column('A:AI', 20, None)

	writer.save()

	return