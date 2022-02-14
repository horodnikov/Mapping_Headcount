import pandas as pd
import sqlalchemy
from config_s import host, user, password, db_name


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
                            "FROM headcount_mapping_structure as h where h.employee_name IS NULL AND h.job_title = 'Vice President, Workforce and Delivery Operations'"

            mapping_dataFrame = pd.read_sql(mapping_query, connection)
            pd.set_option('display.expand_frame_repr', False)

            print(mapping_dataFrame)

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
                        WHEN attrition.exit_invoice_through IS NOT NULL THEN attrition.exit_invoice_through
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
                        <= Headcount.exit_date_with_invoice
                    AND Headcount.employee_name = 'Kuraksa Olga' AND Headcount.job_title = 'Vice President, Workforce and Delivery Operations'
                    """
            frame = pd.read_sql(positions_query, connection)
            pd.set_option('display.expand_frame_repr', False)

            print(frame)

        finally:
            connection.close()
            engine.dispose()

    except Exception as ex:
        print("Connection to DWH refused...")
        print(f"'Exception': {ex}")
