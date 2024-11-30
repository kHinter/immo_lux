def generate_report():
    import pandas as pd
    from datetime import date
    import jinja2
    import matplotlib.pyplot as plt
    import io
    import os
    import logging
    import base64
    import numpy as np

    airflow_home = os.environ["AIRFLOW_HOME"]
    today = str(date.today())

    df_data = {
        "Immotop.lu" : {
            "steps" : {
                "cleaned" : {
                    "df" : pd.read_csv(f"{airflow_home}/dags/data/cleaned/immotop_lu_{today}.csv")
                },
                "enriched" : {
                    "df" : pd.read_csv(f"{airflow_home}/dags/data/enriched/immotop_lu_{today}.csv")
                }
            }
        },
        "Athome.lu" : {
            "steps" : {
                "cleaned" : {
                    "df" : pd.read_csv(f"{airflow_home}/dags/data/cleaned/athome_last3d_{today}.csv")
                },
                "enriched" : {
                    "df" : pd.read_csv(f"{airflow_home}/dags/data/enriched/athome_last3d_{today}.csv")
                }
            }
        }
    }

    #Load the HTML template
    html_template = jinja2.Environment(
        #loader=jinja2.FileSystemLoader("/root/airflow/include/html_report_templates")
        loader=jinja2.FileSystemLoader(f"{airflow_home}/include/html_report_templates")
        
    ).get_template("dq_report_template.html")

    #Generate dynamic informations about each dataset
    for website in df_data:
        missing_columns = []

        #Get the missing columns
        for other_website in df_data:
            if other_website != website:
                last_step = list(df_data[other_website]["steps"].keys())[-1]
                for column in df_data[other_website]["steps"][last_step]["df"].columns:
                    if column not in df_data[website]["steps"][last_step]["df"].columns:
                        missing_columns.append(column)

        df_data[website]["missing_columns"] = missing_columns

        #Get the NotNA count per df column
        cleaned_serie = df_data[website]["steps"]["cleaned"]["df"].notna().sum()
        enriched_serie = df_data[website]["steps"]["enriched"]["df"].notna().sum()

        bar_height = 0.43

        #Correspond to the y coordinates of every series index in the bar plot
        y_cleaned = np.arange(len(cleaned_serie.index))
        y_enriched = np.arange(len(enriched_serie.index))

        #Resize the size of the figure displayed
        plt.figure(figsize=(12, 8))
        
        #Configuration of the horizontal bar plot
        barplot_cleaned = plt.barh(y_cleaned - bar_height/2, cleaned_serie.values, height=bar_height, align="center", label="Cleaned")
        barplot_enriched = plt.barh(y_enriched + bar_height/2, enriched_serie.values, height=bar_height, align="center", label="Enriched")
        #plt.tight_layout()

        plt.yticks(y_cleaned, cleaned_serie.index, fontsize=8)
        plt.yticks(y_enriched, enriched_serie.index, fontsize=8)
        
        plt.bar_label(barplot_cleaned, cleaned_serie.values, label_type="edge", padding=3, fontsize=8)
        plt.bar_label(barplot_enriched, enriched_serie.values, label_type="edge", padding=3, fontsize=8)

        plt.legend()
            
        plt_img_bytes = io.BytesIO()
        plt.savefig(plt_img_bytes, format="png")
        plt_img_bytes.seek(0)
        df_data[website]["img"] = base64.b64encode(plt_img_bytes.read()).decode()
        plt.close()

    today = str(date.today())

    context = {
        "date" : today,
        "df_data" : df_data
    }

    #Complete the template with my dynamic data
    reportText = html_template.render(context)
    
    with open(f"{airflow_home}/dags/reports/dq_report_{today}.html", "w") as f:
        f.write(reportText)