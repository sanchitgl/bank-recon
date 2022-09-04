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
	bank_book['Date'] = pd.to_datetime(bank_book['Date'], format='%m/%d/%Y')

	#-------------------------------------Process Bank Statements-----------------------------------

	bank_statement = pd.read_excel(bank_statement, sheet_name='Details')
	bank_statement['Transaction Date'] = pd.to_datetime(bank_statement['Transaction Date'], format='%m/%d/%y')
	bank_statement['Bank Reference'].fillna('', inplace=True)
	bank_statement['Bank Reference'] = np.where(bank_statement['Bank Reference'] == '', bank_statement['Transaction Date'].dt.strftime('%m/%d/%Y') , bank_statement['Bank Reference'])

	bank_statement['Credit Amount'] = bank_statement['Credit Amount'].round(decimals=2)
	bank_statement['Debit Amount'] = bank_statement['Debit Amount'].round(decimals=2)

	bank_statement.fillna('', inplace=True)
	bank_statement_deposits = bank_statement[bank_statement['Credit Amount'] != '']
	bank_statement_deposits['Credit Amount'] = bank_statement_deposits['Credit Amount'].astype(float)

	bank_statement_withdrawals = bank_statement[bank_statement['Debit Amount'] != '']
	bank_statement_withdrawals['Debit Amount'] = bank_statement_withdrawals['Debit Amount'].astype(float)
	bank_statement_withdrawals['Debit Amount'] = bank_statement_withdrawals['Debit Amount'].round(decimals=2) * (-1)

	#------------------------------------One to one single amounts----------------------------------------
	bank_book['Transaction text'].fillna('', inplace=True)
	bank_book_extract1 = bank_book[bank_book['Transaction text'].str.lower().str.contains('cash concentration transfer')]

	bank_book['Bank Reference'] = ''
	bank_statement_deposits['Journal number'] = ''
	bank_statement_withdrawals['Journal number'] = ''

	indexer1 = recordlinkage.Index()
	indexer1.block(left_on='Amount FC', right_on='Credit Amount')
	comparisons1 = indexer1.index(bank_book_extract1, bank_statement_deposits)
	compare1 = recordlinkage.Compare()
	compare1.exact('Amount FC', 'Credit Amount')
	result1 = compare1.compute(comparisons1, bank_book_extract1, bank_statement_deposits)
	result_reset1 = result1.reset_index()

	if len(result_reset1) > 0:
		unique = result_reset1[~result_reset1.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			for i in range(len(unique)):
				bank_book.at[unique['level_0'].iloc[i], 'Bank Reference'] = bank_statement_deposits.at[unique['level_1'].iloc[i], 'Bank Reference']
				bank_statement_deposits.at[unique['level_1'].iloc[i], 'Journal number'] = bank_book.at[unique['level_0'].iloc[i], 'Journal number']

		duplicate = result_reset1[~result_reset1.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_extract1[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_deposits[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')

				while len(df) > 0:
					bank_book.at[df['level_0'].iloc[0], 'Bank Reference'] = df['Bank Reference'].iloc[0]
					bank_statement_deposits.at[df['level_1'].iloc[0], 'Journal number'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	indexer2 = recordlinkage.Index()
	indexer2.block(left_on='Amount FC', right_on='Debit Amount')
	comparisons2 = indexer2.index(bank_book_extract1, bank_statement_withdrawals)
	compare2 = recordlinkage.Compare()
	compare2.exact('Amount FC', 'Debit Amount')
	result2 = compare2.compute(comparisons2, bank_book_extract1, bank_statement_withdrawals)
	result_reset2 = result2.reset_index()

	if len(result_reset2) > 0:
		unique = result_reset2[~result_reset2.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			for i in range(len(unique)):
				bank_book.at[unique['level_0'].iloc[i], 'Bank Reference'] = bank_statement_withdrawals.at[unique['level_1'].iloc[i], 'Bank Reference']
				bank_statement_withdrawals.at[unique['level_1'].iloc[i], 'Journal number'] = bank_book.at[unique['level_0'].iloc[i], 'Journal number']

		duplicate = result_reset2[~result_reset2.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_extract1[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_withdrawals[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')

				while len(df) > 0:
					bank_book.at[df['level_0'].iloc[0], 'Bank Reference'] = df['Bank Reference'].iloc[0]
					bank_statement_deposits.at[df['level_1'].iloc[0], 'Journal number'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	bank_book_extract2 = bank_book[~bank_book['Transaction text'].str.lower().str.contains('cash concentration transfer')]
	bank_book_grouped = bank_book_extract2.groupby(['Journal number', 'Date']).agg({'Amount FC': 'sum'}).reset_index()
	bank_book_grouped['Amount FC'] = bank_book_grouped['Amount FC'].round(decimals=2)

	#---------------------------------Match Date and Amount--------------------------------------
	indexer3 = recordlinkage.Index()
	indexer3.block(left_on='Amount FC', right_on='Credit Amount')
	comparisons3 = indexer3.index(bank_book_grouped, bank_statement_deposits)
	compare3 = recordlinkage.Compare()
	compare3.exact('Amount FC', 'Credit Amount')
	result3 = compare3.compute(comparisons3, bank_book_grouped, bank_statement_deposits)
	result_reset3 = result3.reset_index()

	if len(result_reset3) > 0:
		unique = result_reset3[~result_reset3.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			for i in range(len(unique)):
				bank_book_grouped.at[unique['level_0'].iloc[i], 'Bank Reference'] = bank_statement_deposits.at[unique['level_1'].iloc[i], 'Bank Reference']
				bank_statement_deposits.at[unique['level_1'].iloc[i], 'Journal number'] = bank_book_grouped.at[unique['level_0'].iloc[i], 'Journal number']

		duplicate = result_reset3[~result_reset3.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_grouped[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_deposits[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])
			for a in list(set(duplicate['Amount Book'].to_list())):

				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')

				while len(df) > 0:
					bank_book.at[df['level_0'].iloc[0], 'Bank Reference'] = df['Bank Reference'].iloc[0]
					bank_statement_deposits.at[df['level_1'].iloc[0], 'Journal number'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	indexer4 = recordlinkage.Index()
	indexer4.block(left_on='Amount FC', right_on='Debit Amount')
	comparisons4 = indexer4.index(bank_book_grouped, bank_statement_withdrawals)
	compare4 = recordlinkage.Compare()
	compare4.exact('Amount FC', 'Debit Amount')
	result4 = compare4.compute(comparisons4, bank_book_grouped, bank_statement_withdrawals)
	result_reset4 = result4.reset_index()

	if len(result_reset4) > 0:
		unique = result_reset4[~result_reset4.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			for i in range(len(unique)):
				bank_book_grouped.at[unique['level_0'].iloc[i], 'Bank Reference'] = bank_statement_withdrawals.at[unique['level_1'].iloc[i], 'Bank Reference']
				bank_statement_withdrawals.at[unique['level_1'].iloc[i], 'Journal number'] = bank_book_grouped.at[unique['level_0'].iloc[i], 'Journal number']

		duplicate = result_reset4[~result_reset4.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_grouped[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_withdrawals[['Transaction Date', 'Bank Reference', 'Credit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Credit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')

				while len(df) > 0:
					bank_book_grouped.at[df['level_0'].iloc[0], 'Bank Reference'] = df['Bank Reference'].iloc[0]
					bank_statement_withdrawals.at[df['level_1'].iloc[0], 'Journal number'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	bank_book_grouped.rename(columns={'Bank Reference': 'Bank Reference2'}, inplace=True)
	bank_book = bank_book.merge(bank_book_grouped[['Journal number', 'Bank Reference2']], on='Journal number', how='left')
	bank_book['Bank Reference'].fillna('', inplace=True)
	bank_book['Bank Reference2'].fillna('', inplace=True)
	bank_book['Bank Reference'] = np.where((bank_book['Bank Reference'] == '') & (bank_book['Bank Reference2'] != ''), bank_book['Bank Reference2'], bank_book['Bank Reference'])
	bank_book.drop(['Bank Reference2'], axis=1, inplace=True)
	bank_statement_appended = bank_statement_deposits.append(bank_statement_withdrawals)
	bank_statement = bank_statement.merge(bank_statement_appended[['Journal number']], left_index=True, right_index=True, how='left')

	bank_book['Date'] = pd.to_datetime(bank_book['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
	bank_statement['Transaction Date'] = bank_statement['Transaction Date'].dt.strftime('%Y-%m-%d')
	bank_statement['Value Date'] = bank_statement['Transaction Date']

	writer = pd.ExcelWriter('temp/master_reconciled.xlsx', engine='xlsxwriter')
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
