import pandas as pd
import sqlalchemy
from config_dwh_dev import dev_host as host, dev_user as user, dev_password as password, dev_db_name as db_name
from config_workbench import loc_host, loc_user, loc_password, loc_db_name


def to_dict(dataframe):
    data = []
    for my_index, my_row in dataframe.iterrows():
        column_data = {}
        for my_key, my_value in my_row.iteritems():
            column_data[my_key] = my_value
        data.append(column_data)
    return data


def sort_by_param(mas, param):
    return sorted(mas, key=lambda x: (str(x).count(f'{param}')), reverse=False)


def structure_sort(lst):
    exclusive = []
    billable = []
    admin = []

    for element in range(len(lst)):
        for index_, row in lst[element].items():
            if index_ == 'employee_name' and str(row) != 'None':
                exclusive.append(lst[element])
            if index_ == 'billing_model_name' and row != 'Admin' and lst[element] not in exclusive \
                    and str(lst[element]['employee_name']) == 'None':
                billable.append(lst[element])
            elif index_ == 'customer_name' and row == 'Maternity Leave' and lst[element] not in billable:
                billable.append(lst[element])
        if lst[element] not in billable and lst[element] not in exclusive:
            admin.append(lst[element])

    exclusive_items = sort_by_param(exclusive, 'None')
    admin_items = sort_by_param(admin, 'None')
    billable_items = sort_by_param(billable, 'None')
    sorted_items = exclusive_items + billable_items + admin_items

    return sorted_items


def merge(sorted_array, position_array):
    mapping_list = []
    position_list = []
    position_len = len(position_array[0])

    name_list = ['billing_model_name', 'revenue_center_name', 'division_name', 'customer_name', 'project_name',
                 'unit_name',
                 'job_title', 'location_name']

    organisation_data = ['division', 'department', 'sub_department']

    for s in range(len(sorted_array)):
        for p in range(len(position_array)):
            if position_len == len(position_array[p]):
                for name in range(len(name_list)):
                    if sorted_array[s][f'{name_list[name]}'] is not None:
                        mapping_list.append(sorted_array[s][f'{name_list[name]}'])
                        position_list.append(position_array[p][f'{name_list[name]}'])
                if mapping_list == position_list:
                    for field in organisation_data:
                        position_array[p][f"{field}"] = sorted_array[s][f"{field}"]

                mapping_list.clear()
                position_list.clear()

    for none_data in range(len(position_array)):
        if len(position_array[none_data]) == 12:
            for field in organisation_data:
                position_array[none_data][f"{field}"] = 'None'

    return position_array


def write_data(position_array):
    headcount_fields = []

    for key in (position_array[0].keys()):
        headcount_fields.append(key)

    d = {}

    for i in range(len(headcount_fields)):
        d[headcount_fields[i]] = []

    for index, value in enumerate(position_array):
        for field in range(len(headcount_fields)):
            d[headcount_fields[field]].append(value[headcount_fields[field]])

    return d


if __name__ == '__main__':

    df = {}
    try:
        connectionString = "mysql+pymysql://" + user + ":" + password + "@" + host + "/" + db_name
        connect_args = {"ssl": {'ca': r"C:\Users\igh\Downloads\BaltimoreCyberTrustRoot.crt.pem"}}

        engine = sqlalchemy.create_engine(connectionString, connect_args=connect_args)
        connection = engine.connect()
        print("successfully connection to DWH...")

        try:
            mapping_query = f"SELECT h.billing_model_name, h.revenue_center_name, h.division_name, h.customer_name, " \
                            "h.project_name, h.unit_name, h.job_title, h.location_name, h.employee_name, h.start_date, " \
                            "h.end_date, h.division, h.department, h.sub_department " \
                            "FROM headcount_mapping_structure as h where h.employee_name IS NULL"

            mapping_dataFrame = pd.read_sql(mapping_query, connection)
            mapping_data = to_dict(mapping_dataFrame)
            sorted_data = structure_sort(mapping_data)

            positions_query = f"""SELECT headcount.*
                FROM (SELECT positions.billing_model_name,
                    positions.revenue_center_name,
                    positions.division_name,
                    positions.customer_name,
                    positions.project_name,
                    positions.unit_name,
                    positions.job_title,
                    positions.location_name,
                    concat(employees.last_name,' ',employees.first_name) AS employee_name,
                    attrition.start_activity_date,
                    CASE
                        WHEN attrition.start_activity_date = attrition.exit_activity_date THEN ADDDATE(attrition.exit_activity_date, INTERVAL 1 DAY)
                        WHEN attrition.exit_activity_date IS NOT NULL AND attrition.exit_invoice_through IS NOT NULL THEN attrition.exit_invoice_through
                        WHEN attrition.exit_activity_date IS NOT NULL AND attrition.exit_invoice_through IS NULL THEN attrition.exit_activity_date
                        WHEN attrition.exit_activity_date IS NULL AND attrition.exit_invoice_through IS NULL
                        THEN ADDDATE(CURDATE(), INTERVAL 1 MONTH)
                    END AS exit_date_with_invoice,
                    attrition.employee_guid
                    FROM fulfillment_attrition AS attrition
                    LEFT JOIN fulfillment_positions positions
                        ON attrition.position_guid = positions.guid
                    LEFT JOIN common_employees AS employees
                        ON attrition.employee_guid = employees.guid ) AS headcount
                    WHERE ADDDATE(ADDDATE(CURDATE(), INTERVAL -4 YEAR), INTERVAL -1 MONTH)
                        <= Headcount.exit_date_with_invoice"""
            positions_dataFrame = pd.read_sql(positions_query, connection)
            positions_data = to_dict(positions_dataFrame)

            positions_data = merge(sorted_data, positions_data)

            array = write_data(positions_data)
            df = pd.DataFrame(array)

        finally:
            connection.close()
            engine.dispose()

    except Exception as ex:
        print("Connection to DWH refused...")
        print(f"'Exception': {ex}")

    try:
        eng = sqlalchemy.create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=loc_host, db=loc_db_name,
                                                                                        user=loc_user, pw=loc_password))
        df.to_sql('headcount', eng, index=False, if_exists='replace')
        print("successfully connection to write...")
        eng.dispose()

    except Exception as ex:
        print("Connection to local database refused...")
        print(f"'Exception': {ex}")
