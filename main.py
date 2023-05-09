import pandas as pd
import sqlalchemy.engine.base
from config_dwh import prod_host, prod_user, prod_password, prod_db
from config_dwh_dev import dev_host, dev_user, dev_password, dev_db
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

ssl_args = {'ssl_ca': '/var/www/html/BaltimoreCyberTrustRoot.crt.pem'}


def connect_sql(username: str, password: str, host: str, database: str):
    """Connect to the SQL Server database"""
    try:
        engine = create_engine(
            "mysql+mysqlconnector://" +
            username + ":" +
            password + "@" +
            host + "/" +
            database,
            connect_args=ssl_args)
        connection = engine.connect()
        print(f"successfully connection to {database}...")
        return connection
    except Exception as exception:
        print(f"Failed to connect! Exception: {exception}")


dev_conn = connect_sql(dev_user, dev_password, dev_host, dev_db)
prod_conn = connect_sql(prod_user, prod_password, prod_host, prod_db)


def wrapped_execute(conn: sqlalchemy.engine.base.Connectable,
                    query: str, data_tuple: tuple, autocommit=True):
    conn.autocommit = autocommit
    try:
        return pd.read_sql(query, conn, params=[data_tuple])
    except Exception as exception:
        print(f"Failed query execution. Exception: {exception}")


def insert_sql(conn: sqlalchemy.engine.base.Connectable,
               table: str, df_input: pd.DataFrame):
    try:
        conn.autocommit = True
        df_input.to_sql(
            name=table,
            con=conn,
            index=False,
            if_exists="append")
        print(f"successfully insert to {table} using {conn}...")
    except IntegrityError:
        print(IntegrityError.__name__)


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
            if index_ == 'billing_model_name' and row != 'Admin' \
                    and lst[element] not in exclusive \
                    and str(lst[element]['employee_name']) == 'None':
                billable.append(lst[element])
            elif index_ == 'customer_name' and row == 'Maternity Leave' \
                    and lst[element] not in billable:
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

    name_list = [
        'billing_model_name', 'revenue_center_name', 'division_name',
        'customer_name', 'project_name', 'unit_name', 'job_title',
        'location_name']
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


def zip_data(position_array):
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
    mapping_query = """SELECT h.billing_model_name, 
                              h.revenue_center_name, 
                              h.division_name, 
                              h.customer_name,
                              h.project_name, 
                              h.unit_name, 
                              h.job_title, 
                              h.location_name, 
                              h.employee_name, 
                              h.start_date,
                              h.end_date, 
                              h.division, 
                              h.department, 
                              h.sub_department 
                    FROM headcount_mapping_structure as h 
                    WHERE h.employee_name IS NULL"""

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
                        WHEN attrition.exit_invoice_through IS NOT NULL 
                            THEN attrition.exit_invoice_through
                        WHEN attrition.start_activity_date = attrition.exit_activity_date 
                            THEN ADDDATE(attrition.exit_activity_date, INTERVAL 1 DAY)
                        WHEN attrition.exit_activity_date IS NOT NULL AND attrition.exit_invoice_through IS NULL 
                            THEN attrition.exit_activity_date
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

    connections = [dev_conn, prod_conn]
    for connection in connections:
        mapping_df = wrapped_execute(connection, mapping_query, ())
        mapping_data = to_dict(mapping_df)
        sorted_data = structure_sort(mapping_data)
        positions_df = wrapped_execute(connection, positions_query, ())
        positions_data = to_dict(positions_df)
        positions_data = merge(sorted_data, positions_data)
        array = zip_data(positions_data)
        df = pd.DataFrame(array)
        insert_sql(connection, 'headcount', df)
