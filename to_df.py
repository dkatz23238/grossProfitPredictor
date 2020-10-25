import json
import pandas as pd
import glob

import datetime

STOCK = "AAPL"


fundamentals_path = r"./financial-securities-data/fundamentals/"
historical_path = r"./financial-securities-data/historical/"
f500 = pd.read_csv("./constituents.csv")

historicals = glob.glob(historical_path + "*.json")
fundamentals = glob.glob(fundamentals_path + "*.json")


dataframes_i = []
dataframes_b = []
dataframes_c = []

for f in f500.Symbol:
    print(f)
    try:
        incomes_statement_records = []
        balance_sheet_records = []
        cash_flow_records = []

        data = json.loads(
            open(f"./financial-securities-data/fundamentals/{f}.json").read())

        quarters = sorted(list(data["data"]["Financials"]
                               ["Income_Statement"]["quarterly"].keys()))

        quarters_dt = [datetime.datetime.strptime(
            i, "%Y-%m-%d") for i in quarters]

        for i, quarter in enumerate(quarters):
            ic = data["data"]["Financials"]["Income_Statement"]["quarterly"][quarter].copy()
            ic["quarter"] = quarters_dt[i]
            ic["source"] = "income_statement"
            ic["ticker"] = f.split("/")[-1].split(".")[0]

            incomes_statement_records.append(ic)
        for i, quarter in enumerate(quarters):
            ic = data["data"]["Financials"]["Balance_Sheet"]["quarterly"][quarter].copy()
            ic["quarter"] = quarters_dt[i]
            ic["source"] = "balance_sheet"
            ic["ticker"] = f.split("/")[-1].split(".")[0]

            balance_sheet_records.append(ic)

        for i, quarter in enumerate(quarters):
            ic = data["data"]["Financials"]["Cash_Flow"]["quarterly"][quarter].copy()
            ic["quarter"] = quarters_dt[i]
            ic["source"] = "cash_flow"
            ic["ticker"] = f.split("/")[-1].split(".")[0]

            cash_flow_records.append(ic)

        incomes = pd.DataFrame(incomes_statement_records).set_index("quarter")
        balances = pd.DataFrame(balance_sheet_records).set_index("quarter")
        cash_flows = pd.DataFrame(cash_flow_records).set_index("quarter")

        # If it quacks it must be a duck
        # https://realpython.com/lessons/duck-typing/
        for col in incomes:
            try:
                incomes[col] = incomes[col].astype(float)
            except Exception as e:
                print(e)

        for col in balances:
            try:
                balances[col] = balances[col].astype(float)
            except Exception as e:
                print(e)

        for col in cash_flows:
            try:
                cash_flows[col] = cash_flows[col].astype(float)
            except Exception as e:
                print(e)

        dataframes_i.append(incomes)
        dataframes_b.append(balances)
        dataframes_c.append(cash_flows)

    except Exception as e:
        print(e)


cdf = pd.concat(dataframes_c, axis=0)
cdf.to_csv("./cashflows-f500.csv")
idf = pd.concat(dataframes_i, axis=0)
idf.to_csv("./incomestatements-f500.csv")
bdf = pd.concat(dataframes_b, axis=0)
bdf.to_csv("./balancesheets-f500.csv")

bdf["index"] = bdf.reset_index()["quarter"].apply(
    lambda x: x.isoformat()).values + bdf.ticker.values

cdf["index"] = cdf.reset_index()["quarter"].apply(
    lambda x: x.isoformat()).values + cdf.ticker.values

idf["index"] = idf.reset_index()["quarter"].apply(
    lambda x: x.isoformat()).values + idf.ticker.values

all_financials = idf.merge(cdf, on="index").merge(bdf, on="index")

bad_cols = [i for i in all_financials.columns if ("_x" in i or "_y" in i)]


for b in bad_cols:
    del all_financials[b]

all_financials.to_csv("all-financials-f500.csv")
