import os
from apiclient import discovery
from google.oauth2 import service_account
import requests
import xml.etree.ElementTree as ET
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe


try:
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file",
              "https://www.googleapis.com/auth/spreadsheets"]
    secret_file = os.path.join(os.getcwd(), 'client_secret.json')

    spreadsheet_id = '153bTV4DHvn4FzviU_vtu_OOMHSnbyzTp02GzT_iK0Rk'
    range_name = 'A1:J1'

    credentials = service_account.Credentials.from_service_account_file(secret_file, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    rangeAll = 'Hoja 1!A1:Z'
    body = {}
    resultClear = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=rangeAll,
                                                        body=body).execute()

except OSError as e:
    print(e)


def get_country_info(country_code):
    link = "http://tarea-4.2021-1.tallerdeintegracion.cl/gho_%s.xml" % country_code
    response = requests.get(link)
    string_xml = response.content
    tree = ET.ElementTree(ET.fromstring(string_xml))
    root = tree.getroot()
    params_required = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths",
                       "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
                       "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
                       "Estimates of number of homicides", "Crude suicide rates (per 100 000 population)",
                       "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
                       "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
                       "Estimated road traffic death rate (per 100 000 population)", "Estimated number of road traffic deaths",
                       "Mean BMI (kg/m&#xb2;) (crude estimate)", "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
                       "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)",
                       "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
                       "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
                       "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
                       "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
                       "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
                       "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
                       "Estimate of daily cigarette smoking prevalence (%)", "Estimate of daily tobacco smoking prevalence (%)",
                       "Estimate of current cigarette smoking prevalence (%)", "Estimate of current tobacco smoking prevalence (%)",
                       "Mean systolic blood pressure (crude estimate)", "Mean fasting blood glucose (mmol/l) (crude estimate)",
                       "Mean Total Cholesterol (crude estimate)"]
    
    columns = ["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "Display", "Numeric", "High", "Low"]
    all_items = []

    for child in root:
        gho = child.findtext("GHO")
        present = False
        for param in params_required:
            # print(gho, param)
            if (gho in param) or (param in gho):
                # print("Gho es: ", gho)
                present = True
        country = child.findtext("COUNTRY")
        sex = child.findtext("SEX")
        year = child.findtext("YEAR")
        ghecauses = child.findtext("GHECAUSES")
        agegroup = child.findtext("AGEGROUP")
        display = child.findtext("Display")
        numeric = child.findtext("Numeric")
        high = child.findtext("High")
        low = child.findtext("Low")

        store_items = [gho, country, sex, year, ghecauses, agegroup, display, numeric, high, low]
        for i in range(len(store_items)):
            check = store_items[i]
            # print(check, type(check))
            if check is not None:
                try:
                    store_items[i] = float(check)

                except ValueError:
                    pass
        if present:
            all_items.append(store_items)

    df = pd.DataFrame(all_items, columns=columns)

    return df


# Variable global con paises a cargar
ID_PAISES = ["MEX", "COL", "CHL", "BRA", "PER", "URY"]


def populate_sheet():
    data = []
    for country in ID_PAISES:
        df = get_country_info(country)
        data.append(df)
    new_df = pd.concat(data)

    spreadsheetId = '153bTV4DHvn4FzviU_vtu_OOMHSnbyzTp02GzT_iK0Rk'  # Please set the Spreadsheet ID.
    client = gspread.authorize(credentials)
    sh = client.open_by_key(spreadsheetId)
    wksheet = sh.get_worksheet(0)
    set_with_dataframe(wksheet, new_df)
    return


populate_sheet()
