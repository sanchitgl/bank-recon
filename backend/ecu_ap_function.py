import pandas as pd
import os
import sys
import re
import numpy as np
import recordlinkage

def reconcile(bank_book, bank_statement_, previous_reco):
	error = ""
	#-------------------------------------Process Bank Statements-----------------------------------

	bank_statement = pd.read_excel(bank_statement_, sheet_name='Details')
	bank_statement['Transaction Date'] = pd.to_datetime(bank_statement['Transaction Date'], format='%m/%d/%Y')
	bank_statement['Debit Amount'] = bank_statement['Debit Amount'].astype(float).round(decimals=2)
	bank_statement['Credit Amount'] = bank_statement['Credit Amount'].astype(float).round(decimals=2)
	bank_statement_ach = bank_statement[bank_statement['Description'] == 'DEBIT ACH SETTLEMENT']
	bank_statement_achitems = pd.read_excel(bank_statement_, sheet_name='ACH Items')
	bank_statement_achitems['Trace Number'] = bank_statement_achitems['Trace Number'].astype(str)
	bank_statement_achitems['Journal'] = bank_statement_achitems['Trace Number'].str[:6] + '_' + bank_statement_achitems['Trace Number'].str[6:9]

	if not round(bank_statement_ach['Debit Amount'].sum(), 2) == round(bank_statement_achitems['Item Amount'].sum(), 2):
		#print(round(bank_statement_ach['Debit Amount'].sum(), 2))
		#print(round(bank_statement_achitems['Item Amount'].sum(), 2))
		error = "ERROR: ACH totals do not match in 'Details' sheet and 'ACH Items' sheet"

		return error

	bank_statement_checks = bank_statement[bank_statement['Description'] == 'CHECK']
	bank_statement_checks['Customer Reference'] = bank_statement_checks['Customer Reference'].str[-6:]

	bank_statement['Debit Amount'].fillna('', inplace=True)
	bank_statement_withdrawals = bank_statement[(bank_statement['Debit Amount'] != '') & (bank_statement['Description'] != 'CHECK') & (bank_statement['Description'] != 'DEBIT ACH SETTLEMENT')]

	bank_statement['Credit Amount'].fillna('', inplace=True)
	bank_statement_deposits = bank_statement[bank_statement['Credit Amount'] != '']

	bank_book = pd.read_excel(bank_book, skiprows=4)
	bank_book.dropna(subset=['Amount FC'], inplace=True)
	bank_book = bank_book[(bank_book['Amount FC'] != 'Amount FC')]
	bank_book['Amount FC'] = bank_book['Amount FC'].astype(float).round(decimals=2)
	bank_book['Date'] = pd.to_datetime(bank_book['Date'], format='%m/%d/%Y')
	bank_book['Journal number'].fillna(bank_book['Voucher'], inplace=True)

	bank_book_checks = bank_book[bank_book['Method of payment'] == 'Cheque']

	#---------------------------------Cheques--------------------------------------
	bank_statement_checks['Debit Amount'] = bank_statement_checks['Debit Amount'] * (-1)
	bank_book_checks['Bank Details'] = bank_book_checks['Bank Details'].astype(str)
	bank_statement_checks['Customer Reference'] = bank_statement_checks['Customer Reference'].astype(str)

	indexer1 = recordlinkage.Index()
	indexer1.block(left_on=['Bank Details', 'Amount FC'], right_on=['Customer Reference', 'Debit Amount'])
	comparisons1 = indexer1.index(bank_book_checks, bank_statement_checks)
	compare1 = recordlinkage.Compare()
	compare1.exact('Amount FC', 'Debit Amount')
	compare1.exact('Bank Details', 'Customer Reference')
	result1 = compare1.compute(comparisons1, bank_book_checks, bank_statement_checks)
	result_reset1 = result1.reset_index()

	if len(result_reset1) > 0:
		unique = result_reset1[~result_reset1.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book_checks[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_checks[['Bank Reference']], left_on='level_1', right_index=True)

			bank_book_checks = pd.merge(bank_book_checks, unique[['level_0', 'Bank Reference']], left_index=True, right_on='level_0', how='outer').set_index('level_0')
			bank_statement_checks = pd.merge(bank_statement_checks, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').set_index('level_1')

		duplicate = result_reset1[~result_reset1.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_checks[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_checks[['Transaction Date', 'Bank Reference', 'Debit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Debit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])

			list_a = []
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				while len(duplicate) > 0:
					bank_book_checks['Bank Reference'].iloc[duplicate['level_0'].iloc[0]] = duplicate['Bank Reference'].iloc[0]
					bank_statement_checks['Journal number'].iloc[duplicate['level_1'].iloc[0]] = duplicate['Journal number'].iloc[0]

					duplicate = duplicate[(duplicate['level_0'] != duplicate['level_0'].iloc[0]) & (duplicate['level_1'] != duplicate['level_1'].iloc[0])]
	else:
		bank_book_checks['Bank Reference'] = ''
		bank_statement_checks['Journal number'] = ''

	bank_book = bank_book.merge(bank_book_checks[['Bank Reference']], left_index=True, right_index=True, how='left').rename(columns={'Bank Reference': 'Bank Reference 1'})
	bank_statement = bank_statement.merge(bank_statement_checks[['Journal number']], left_index=True, right_index=True, how='left').rename(columns={'Journal number': 'Journal number 1'})

	#----------------------------------------ACH---------------------------------------------------

	bank_statement_achitems['Item Amount'] = bank_statement_achitems['Item Amount'] * (-1)
	indexer2 = recordlinkage.Index()
	indexer2.block(left_on=['Journal number', 'Amount FC'], right_on=['Journal', 'Item Amount'])
	comparisons2 = indexer2.index(bank_book, bank_statement_achitems)
	compare2 = recordlinkage.Compare()
	compare2.exact('Journal number', 'Journal')
	compare2.exact('Amount FC', 'Item Amount')
	result2 = compare2.compute(comparisons2, bank_book, bank_statement_achitems)
	result_reset2 = result2.reset_index()

	if len(result_reset2) > 0:
		unique = result_reset2[~result_reset2.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_achitems[['Trace Number']], left_on='level_1', right_index=True)

			bank_book = pd.merge(bank_book, unique[['level_0', 'Trace Number']], left_index=True, right_on='level_0', how='outer').set_index('level_0').rename(columns={'Trace Number': 'Bank Reference 2'})
			bank_statement_achitems = pd.merge(bank_statement_achitems, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').set_index('level_1').rename(columns={'Journal number': 'Journal number 2'})

		duplicate = result_reset2[~result_reset2.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_achitems[['Transaction Date', 'Trace Number', 'Item Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Item Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])

			list_a = []
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				while len(df) > 0:
					bank_book.at[df['level_0'].iloc[0], 'Bank Reference 2'] = df['Trace Number'].iloc[0]
					bank_statement_achitems.at[df['level_1'].iloc[0], 'Journal number 2'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	else:
		bank_book['Bank Reference 2'] = ''
		bank_statement_achitems['Journal number 2'] = ''
	
	bank_statement['Journal number 2'] = np.where(bank_statement['Description'] == 'DEBIT ACH SETTLEMENT', 'Refer ACH Items Sheet', '')

	#---------------------------------------------Withdrawals-----------------------------------------------
	bank_book['Bank Reference 1'].fillna('', inplace=True)
	bank_book['Bank Reference 2'].fillna('', inplace=True)
	bank_book_withdrawals = bank_book[(bank_book['Bank Reference 1'] == '') & (bank_book['Bank Reference 2'] == '')]
	bank_book_grouped = bank_book_withdrawals.groupby(['Journal number']).agg({'Amount FC': 'sum', 'Date': 'mean'}).reset_index()

	bank_statement_withdrawals['Debit Amount'] = bank_statement_withdrawals['Debit Amount'] * (-1)
	indexer3 = recordlinkage.Index()
	indexer3.block(left_on=['Amount FC'], right_on=['Debit Amount'])
	comparisons3 = indexer3.index(bank_book_grouped, bank_statement_withdrawals)
	compare3 = recordlinkage.Compare()
	compare3.exact('Amount FC', 'Debit Amount')
	result3 = compare3.compute(comparisons3, bank_book_grouped, bank_statement_withdrawals)
	result_reset3 = result3.reset_index()

	if len(result_reset3) > 0:
		unique = result_reset3[~result_reset3.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book_grouped[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_withdrawals[['Bank Reference']], left_on='level_1', right_index=True)

			bank_book_grouped = pd.merge(bank_book_grouped, unique[['level_0', 'Bank Reference']], left_index=True, right_on='level_0', how='outer').set_index('level_0').rename(columns={'Bank Reference': 'Bank Reference 3'})
			bank_statement_withdrawals = pd.merge(bank_statement_withdrawals, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').set_index('level_1').rename(columns={'Journal number': 'Journal number 3'})

		duplicate = result_reset3[~result_reset3.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_grouped[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_withdrawals[['Transaction Date', 'Bank Reference', 'Debit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Debit Amount': 'Amount Statement'})
			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])

			list_a = []
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				df['level_0'] = df['level_0'].astype(int)
				df['level_1'] = df['level_1'].astype(int)

				while len(df) > 0:
					bank_book_grouped.at[df['level_0'].iloc[0], 'Bank Reference 3'] = df['Bank Reference'].iloc[0]
					bank_statement_withdrawals.at[df['level_1'].iloc[0], 'Journal number 3'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	else:
		bank_book_grouped['Bank Reference 3'] == ''
		bank_statement_withdrawals['Journal number 3'] = ''

	bank_book_withdrawals = bank_book_withdrawals.reset_index().merge(bank_book_grouped[['Journal number', 'Bank Reference 3']], on='Journal number', how='left')
	bank_book = bank_book.merge(bank_book_withdrawals[['level_0', 'Bank Reference 3']], left_index=True, right_on='level_0', how='left')
	bank_statement = bank_statement.merge(bank_statement_withdrawals[['Journal number 3']], left_index=True, right_index=True, how='left')

	#-----------------------------------------------Deposits-----------------------------------------------------

	bank_book['Bank Reference 1'].fillna('', inplace=True)
	bank_book['Bank Reference 3'].fillna('', inplace=True)
	bank_statement['Journal number 1'].fillna('', inplace=True)
	bank_statement['Journal number 2'].fillna('', inplace=True)
	bank_book_deposits = bank_book[(bank_book['Bank Reference 1'] == '') & (bank_book['Bank Reference 3'] == '') & (bank_book['Amount FC'] > 0)]

	indexer3 = recordlinkage.Index()
	indexer3.block(left_on=['Amount FC'], right_on=['Credit Amount'])
	comparisons3 = indexer3.index(bank_book_deposits, bank_statement_deposits)
	compare3 = recordlinkage.Compare()
	compare3.exact('Amount FC', 'Credit Amount')
	result3 = compare3.compute(comparisons3, bank_book_deposits, bank_statement_deposits)
	result_reset3 = result3.reset_index()

	if len(result_reset3) > 0:
		unique = result_reset3[~result_reset3.duplicated('level_0', keep=False)]
		unique = unique[~unique.duplicated('level_1', keep=False)]

		if len(unique) > 0:
			unique = pd.merge(unique, bank_book_deposits[['Journal number']], left_on='level_0', right_index=True)
			unique = pd.merge(unique, bank_statement_deposits[['Bank Reference']], left_on='level_1', right_index=True)

			bank_book_deposits = pd.merge(bank_book_deposits, unique[['level_0', 'Bank Reference']], left_index=True, right_on='level_0', how='outer').set_index('level_0').rename(columns={'Bank Reference': 'Bank Reference 4'})
			bank_statement_deposits = pd.merge(bank_statement_deposits, unique[['level_1', 'Journal number']], left_index=True, right_on='level_1', how='outer').set_index('level_1').rename(columns={'Journal number': 'Journal number 4'})

		duplicate = result_reset3[~result_reset3.isin(unique)].dropna()

		if len(duplicate) > 0:
			duplicate = pd.merge(duplicate, bank_book_deposits[['Date', 'Journal number', 'Amount FC']], left_on='level_0', right_index=True).rename(columns={'Date': 'Date Book', 'Amount FC': 'Amount Book'})
			duplicate = pd.merge(duplicate, bank_statement_deposits[['Transaction Date', 'Bank Reference', 'Debit Amount']], left_on='level_1', right_index=True).rename(columns={'Transaction Date': 'Date Statement', 'Debit Amount': 'Amount Statement'})

			duplicate['Days Difference'] = abs(duplicate['Date Book'] - duplicate['Date Statement'])

			list_a = []
			for a in list(set(duplicate['Amount Book'].to_list())):
				df = duplicate[duplicate['Amount Book'] == a]
				df = df.sort_values(by='Days Difference')
				df['level_0'] = df['level_0'].astype(int)
				df['level_1'] = df['level_1'].astype(int)

				while len(df) > 0:
					bank_book_deposits.at[df['level_0'].iloc[0], 'Bank Reference 4'] = df['Bank Reference'].iloc[0]
					bank_statement_deposits.at[df['level_1'].iloc[0], 'Journal number 4'] = df['Journal number'].iloc[0]

					df = df[(df['level_0'] != df['level_0'].iloc[0]) & (df['level_1'] != df['level_1'].iloc[0])]

	else:
		bank_book_deposits['Bank Reference 4'] = ''
		bank_statement_deposits['Journal number 4'] = ''

	bank_book = bank_book.merge(bank_book_deposits[['Bank Reference 4']], left_index=True, right_index=True, how='left')
	bank_statement = bank_statement.merge(bank_statement_deposits[['Journal number 4']], left_index=True, right_index=True, how='left')

	bank_book['Bank Reference'] = np.where(bank_book['Bank Reference 1'].str.len() > 0, bank_book['Bank Reference 1'], np.where(bank_book['Bank Reference 2'].str.len() > 0, bank_book['Bank Reference 2'], np.where(bank_book['Bank Reference 3'].str.len() > 0, bank_book['Bank Reference 3'], np.where(bank_book['Bank Reference 4'].str.len() > 0, bank_book['Bank Reference 4'], ''))))
	bank_statement['Journal number'] = np.where(bank_statement['Journal number 1'].str.len() > 0, bank_statement['Journal number 1'], np.where(bank_statement['Journal number 2'].str.len() > 0, bank_statement['Journal number 2'], np.where(bank_statement['Journal number 3'].str.len() > 0, bank_statement['Journal number 3'], np.where(bank_statement['Journal number 4'].str.len() > 0, bank_statement['Journal number 4'], ''))))
	bank_statement_achitems = bank_statement_achitems.rename(columns={'Journal number 2': 'Journal number'})

	bank_book.drop(['Bank Reference 1', 'Bank Reference 2', 'level_0', 'Bank Reference 3', 'Bank Reference 4'], axis=1, inplace=True)
	bank_statement.drop(['Journal number 1', 'Journal number 2', 'Journal number 3', 'Journal number 4'], axis=1, inplace=True)
	bank_statement_achitems.drop(['Journal'], axis=1, inplace=True)

	bank_book['Date'] = pd.to_datetime(bank_book['Date'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')
	bank_statement['Transaction Date'] = bank_statement['Transaction Date'].dt.strftime('%Y-%m-%d')
	bank_statement['Value Date'] = bank_statement['Transaction Date']
	bank_statement_achitems['Transaction Date'] = bank_statement_achitems['Transaction Date'].dt.strftime('%Y-%m-%d')

	writer = pd.ExcelWriter('temp/ap_reconciled.xlsx', engine='xlsxwriter')
	bank_book.to_excel(writer, sheet_name='Bank Book', index=False)
	bank_statement.to_excel(writer, sheet_name='Bank Statement Details', index=False)
	bank_statement_achitems.to_excel(writer, sheet_name='Bank Statement ACH Items', index=False)

	workbook = writer.book
	fail_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
	pass_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
	neutral_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C5700'})

	sheet1 = writer.sheets['Bank Book']
	sheet2 = writer.sheets['Bank Statement Details']
	sheet3 = writer.sheets['Bank Statement ACH Items']
	sheet1.conditional_format('V2:V'+str(len(bank_book)+1), {'type': 'blanks', 'format': fail_format})
	sheet1.conditional_format('V2:V'+str(len(bank_book)+1), {'type': 'no_blanks', 'format': pass_format})
	sheet2.conditional_format('AI2:AI'+str(len(bank_statement)+1), {'type': 'text', 'criteria': 'containing', 'value': 'Refer ACH Items Sheet', 'format': neutral_format})
	sheet2.conditional_format('AI2:AI'+str(len(bank_statement)+1), {'type': 'blanks', 'format': fail_format})
	sheet2.conditional_format('AI2:AI'+str(len(bank_statement)+1), {'type': 'no_blanks', 'format': pass_format})
	sheet3.conditional_format('M2:M'+str(len(bank_statement_achitems)+1), {'type': 'blanks', 'format': fail_format})
	sheet3.conditional_format('M2:M'+str(len(bank_statement_achitems)+1), {'type': 'no_blanks', 'format': pass_format})

	sheet1.set_column('A:V', 20, None)
	sheet2.set_column('A:AI', 20, None)
	sheet3.set_column('A:M', 20, None)

	writer.save()

	return error

