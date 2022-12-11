import pandas as pd
import json
from typing import Literal


def get_result_json(file_name: str) -> None:
    wrapper = {
        "allocation": {
            "data": []
        }
    }

    with open(file_name, "r") as read_file:
        json_inner = json.load(read_file)

    wrapper["allocation"]["data"] = json_inner
    result_json = json.dumps(wrapper, indent=4)

    with open(file_name, "w") as save_file:
        save_file.write(result_json)


def main():
    file = 'well_data.xlsx'
    try:
        df_invalid_splits = pd.read_excel(file, sheet_name='invalid_splits')
    except FileNotFoundError:
        raise FileNotFoundError('No such file in directory: "well_data.xlsx"')

    subset_to_group = ['dt', 'well_id']
    columns_for_sum = ['oil_split', 'gas_split', 'water_split']
    df_grouped_table = df_invalid_splits.groupby(subset_to_group)[columns_for_sum].sum()

    CONDITION_NOT_EQUAL_100 = ((df_grouped_table['oil_split'] != 100) |
                               (df_grouped_table['gas_split'] != 100) |
                               (df_grouped_table['water_split'] != 100))

    selection_invalid_splits = df_grouped_table.loc[CONDITION_NOT_EQUAL_100]
    selection_invalid_splits.to_csv('selection_invalid_splits.csv', header=True, index=True)
    df_rates = pd.read_excel(file, sheet_name='rates')
    df_splits = pd.read_excel(file, sheet_name='splits')
    split_rate = pd.merge(df_splits, df_rates, on=('well_id', 'dt'))
    FLUID = Literal['oil', 'gas', 'water']

    def get_split_rate(fluid: FLUID) -> float:
        dict_fluid = {'oil': ['split_rate_oil', 'oil_split', 'oil_rate'],
                      'gas': ['split_rate_gas', 'gas_split', 'gas_rate'],
                      'water': ['split_rate_water', 'water_split', 'water_rate']}
        result = split_rate[dict_fluid[fluid][0]] = \
            split_rate[dict_fluid[fluid][1]] * split_rate[dict_fluid[fluid][2]] / 100
        return result

    get_split_rate('oil')
    get_split_rate('gas')
    get_split_rate('water')
    column_names_excel = ['well_id', 'dt', 'layer_id', 'split_rate_oil', 'split_rate_gas', 'split_rate_water']
    split_rate = split_rate[column_names_excel]
    split_rate.to_csv('allocation.csv', header=True, index=False)

    try:
        split_rate.to_excel('allocation.xlsx', header=True, index=False)
    except PermissionError:
        raise PermissionError('This file is in use by another process. Please close the file and try again.')

    for_json = split_rate.astype({'dt': str})
    column_names_in_camel_case = ['wellId', 'dt', 'layerID', 'oilRate', 'gasRate', 'waterRate']
    for_json.columns = column_names_in_camel_case
    with open("allocation.json", "w") as write_file:
        write_file.write(for_json.to_json(orient='records', indent=4))
    get_result_json("allocation.json")


if __name__ == '__main__':
    main()
