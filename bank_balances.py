import pandas as pd  # version 1.0.1
# in pandas 1.1.4 dates for INTESA and BMG doesn't work after merge in "final"
from datetime import datetime

# TODO find repetitions and replace them with functions
# for example Santander and CITI files import and adjustment
# or date and amount formatting
pd.options.display.float_format = '{:,.2f}'.format
# don't hide columns
pd.set_option('display.expand_frame_repr', False)

# import list of bank accounts into data frame
balances = pd.read_csv('list_of_accounts.csv', sep=';')
balances = balances.set_index('Account')

print('tabelka z listą wszystkich aktywnych rachunków\n')
print(balances)
print('\nlista kolumn\n')
print(balances.columns)
print()

# import bits of information from ING
ing = pd.read_csv('ING_transakcje_zamkniecie.csv', sep=';', encoding='ANSI',
                  usecols=['rachunek ING "NRB" (26 znaków)', 'saldo końcowe',
                           'waluta operacji', 'data wyciągu'])

print('\nUWAGA: plik z ING czasami zawiera błedy. Kolumny się rozjeżdzają. '
      'Trzeba poprawić w excelu.\n')
print('\nING bez przeróbek\n')
print(ing)

ing = ing.rename(columns={'rachunek ING "NRB" (26 znaków)': "Account",
                          "saldo końcowe": "saldo",
                          'data wyciągu': 'data',
                          'waluta operacji': 'Currency'})

ing = ing.set_index('Account')

print('\nING po pierwszych przeróbkach\n')
print(ing)
print(ing['saldo'])

# amount format adjusted
# empty cells need te be removed before next steps to avoid errors
ing = ing.dropna(subset=['saldo'])
ing['saldo'] = ing['saldo'].apply(lambda x: x.replace(',', '.'))
print(ing['saldo'])
ing['saldo'] = pd.to_numeric(ing['saldo'])

# date format adjusted
ing['data'] = pd.to_datetime(ing['data'], format='%y-%m-%d')

# sorting is necessary to catch the newest values
ing.sort_values(by=['data'], inplace=True, ascending=False)
print()

# index has to be removed for a while to delete the duplicates
ing = ing.reset_index().drop_duplicates(subset='Account',
                                        keep='first').set_index('Account')

print('\nDane z ING bez duplikatów wedłgu powtórzeń w indeksie\n', ing, '\n')

print()

# import bits of information from CITI bank
citifilename = 'CITI_salda_zamkniecie.csv'
colnames = ['Account', 'klient', 'saldo', 'Currency', 'data',
            'nazwa_rach', 'nazawa_od', 'oddzial']

citi = pd.read_csv(citifilename, names=colnames, skiprows=1,
                   parse_dates=True, dayfirst=True)

citi = citi.drop(['klient', 'nazwa_rach', 'nazawa_od', 'oddzial'], axis=1)

# date format adjusted
citidtm = lambda x: datetime.strptime(str(x), "%d/%m/%Y")  # 02/08/2019
citi['data'] = citi['data'].apply(citidtm)
citi['data'] = pd.to_datetime(citi['data'])

citi['Account'] = citi['Account'].apply(lambda x: x.replace(' ', ''))
citi = citi.set_index('Account')

print('\nsprawdzam co się wczytuje z CITI\n', citi, '\n')

# import bits of information from Santander bank
# "skiprows" need to be updated if we close or open some bank accounts
santanderfilename = 'Santander_salda_zamkniecie.csv'
san = pd.read_csv(santanderfilename, skiprows=[0, 1, 17, 18, 19],
                  usecols=['Data', 'Numer rachunku', 'Saldo', 'Unnamed: 8'],
                  parse_dates=True, sep=';', encoding='ANSI', )

santandervatfilename = 'Santander_VAT_salda_zamkniecie.csv'
sanvat = pd.read_csv(santandervatfilename, skiprows=[0, 1, 6, 7, 8],
                     usecols=['Data', 'Numer rachunku', 'Saldo', 'Unnamed: 8'],
                     parse_dates=True, sep=';', encoding='ANSI', )



san_tot = pd.concat([san,sanvat])

print()
print(san_tot)
print()

san_tot = san_tot.rename(columns={'Numer rachunku': "Account",
                          "Saldo": "saldo",
                          'Data': 'data',
                          'Unnamed: 8': 'Currency'})

print('zmienione nazwy kolumn')
print(san_tot)
print()


san_tot['saldo'] = san_tot['saldo'].apply(lambda x: x.replace(' ', ''))
san_tot['saldo'] = san_tot['saldo'].apply(lambda x: x.replace(',', '.'))
san_tot['saldo'] = pd.to_numeric(san_tot['saldo'])

san_tot['Account'] = san_tot['Account'].apply(lambda x: x.replace(' ', ''))

san_tot = san_tot.set_index('Account')

san_tot['data'] = pd.to_datetime(san_tot['data'], format='%Y-%m-%d')

# In Santander file the date is only in the first row.
# It must be added into the next rows
san_tot['data'] = san_tot.fillna(method="ffill")
print()
print('sprawdzam co mamy w Santanderze\n', san_tot, '\n')


# import bits of information from Santander bank
bmgfilename = 'BMG_salda_zamkniecie.csv'
bmg = pd.read_csv(bmgfilename, skiprows=range(0, 15),
                  usecols=['Account number', 'Currency',
                           'Closing', 'Closing book balance'],
                  parse_dates=True, sep=';', encoding='ANSI', )

bmg = bmg.rename(
    columns={'Account number': "Account", "Closing book balance": "saldo",
             'Closing': 'data'})
bmg = bmg.set_index('Account')

bmg['data'] = pd.to_datetime(bmg['data'],
                             format='%Y-%m-%d')

print('\nsprawdzam co się wczytuje z BMG\n\n', bmg, '\n\n')



# import bits of information from INTESA bank
intesafilename = 'INTESA_salda_zamkniecie.csv'
intesa = pd.read_csv(intesafilename, parse_dates=True, sep=';', encoding='ANSI')

intesa = intesa.set_index('Account')

intesa['data'] = pd.to_datetime(intesa['data'], format='%Y-%m-%d')

print('\nsprawdzam co się wczytuje z INTESY\n\n', intesa, '\n\n')

# merge all tables
print('\npolaczone tabele\n')

final = balances.merge(ing[['data', 'saldo']], on='Account', how='outer')
final = final.fillna(citi)
final = final.fillna(san_tot)
final = final.fillna(bmg)
final = final.fillna(intesa)

# date format corrected
print()
print(final)
print()

print('final')
print(final['data'])
print()

final['data'] = pd.to_datetime(final['data'].dt.strftime('%Y/%m/%d'))

print(final)

# Add deposits from CITI
citidepofilename = 'CITI_depozyty_zamkniecie.csv'
colnames = ['Account', 'klient', 'depozyt', 'Currency', 'opis', 'beneficjent',
            'data', 'data_waluty', 'bzdet_1', 'bzdet_2']

citidep = pd.read_csv(citidepofilename, names=colnames, skiprows=1,
                      parse_dates=True, dayfirst=True)

citidep = citidep.drop(
    ['klient', 'Currency', 'opis', 'beneficjent', 'data_waluty', 'bzdet_1',
     'bzdet_2'], axis=1)


citidtm = lambda x: datetime.strptime(str(x), "%d/%m/%Y")
citidep['data'] = citidep['data'].apply(citidtm)
citidep['data'] = pd.to_datetime(citidep['data'])

citidep['Account'] = citidep['Account'].apply(lambda x: x.replace(' ', ''))

citidep = citidep.set_index('Account')

# deposit amounts are negative. It have to be changed.
citidep['depozyt'] *= -1

print('\nsprawdzam co się wczytuje z CITI depozyty\n', citidep, '\n')

final = final.merge(citidep['depozyt'], on='Account', how='outer')

# I need zeros instead of empty cells to make calculations
final['depozyt'] = final['depozyt'].fillna(0)
final['saldo'] = final['saldo'].fillna(0)

print()
print(final, end='\n\n')

final['total_balance'] = final.apply(lambda x: x['saldo'] + x['depozyt'],
                                     axis=1)
print()
print(final, end='\n\n')

rates = pd.read_csv('kursy_zamkniecie.csv')
rates = rates.set_index('para')

print(rates, end='\n\n')


def na_pln(currency, total_balance, rate_eur, rate_usd):
    """Calculate the values in PLN"""
    if 'EUR' in currency:
        return total_balance * rate_eur
    elif 'USD' in currency:
        return total_balance * rate_usd
    else:
        return total_balance


fx_eur = rates.at['eur/pln', 'rate']
fx_usd = rates.at['usd/pln', 'rate']

print(fx_eur)
print(fx_usd)

final['total_pln'] = final.apply(
    lambda x: na_pln(x['Currency'], x['total_balance'], fx_eur, fx_usd),
    axis=1)

print()
print(final[['total_balance', 'Currency', 'total_pln']], end='\n\n')


def na_eur(total_pln, rate_eur):
    """Calculate values in EUR"""
    return total_pln / rate_eur


final['total_eur'] = final.apply(lambda x: na_eur(x['total_pln'], fx_eur),
                                 axis=1)


def na_usd(total_pln, rate_usd):
    """Calculate values in USD"""
    return total_pln / rate_usd


final['total_usd'] = final.apply(lambda x: na_usd(x['total_pln'], fx_usd),
                                 axis=1)

print()
print(final[['total_balance', 'Currency', 'total_pln', 'total_eur',
             'total_usd']], end='\n\n')

# import balances from SAP
sap_colnames = ['SAP_comp', 'GL', 'SAP_amount']
sap = pd.read_excel('SAP_GL.xlsx', usecols="A,B,I", names=sap_colnames,
                    converters={'SAP_comp': str, 'GL': str})
print(sap.head(10))
print()


def utnij(konto):
    s_konto = str(konto)
    return s_konto[:-1]


sap['GL'] = sap.apply(lambda x: utnij(x['GL']), axis=1)


def polacz(jed, konto):
    s_jed = str(jed)
    s_konto = str(konto)
    return s_jed + '_' + s_konto


sap['GL_account'] = sap.apply(lambda x: polacz(x['SAP_comp'], x['GL']),
                              axis=1)

sap = sap.set_index('GL_account')

print(sap.head(10))
sap = sap.drop(['SAP_comp', 'GL'], axis=1)
sap = sap.groupby('GL_account').sum()

print()
print(sap.head(10))
print()

print('\nprzed mergem\n')
print(final)
print()

# UWAGA: trzeba po drodze na chwilę usunąć index. bez tego merge likwiduje
# index!!!! który jest numerem rachunku
# dorzucam how='left' bo bez tego usuwa wiersze, które nie miały konta GL

# index must be reset before merging, otherways it would disappear
final = final.reset_index().merge(sap, on='GL_account', how='left')

print('\npo mergu\n')
print(final)
print()

print('\nprzywrócenie indexu\n')
final = final.set_index('Account')

print(final)
print()

final_bez_purpose = final.drop(['Purpose'], axis=1)

print(final_bez_purpose)
print()

final_types = final[['Type', 'total_pln', 'total_eur', 'total_usd']]
final_types = final_types.groupby('Type').sum()
print(final_types)
print()

print('export dwóch tabel do csv')
final.to_csv('output.csv')
final_types.to_csv('output_types.csv')


