[1mdiff --git a/.gitignore b/.gitignore[m
[1mindex 9a18f6b..daf7b95 100644[m
[1m--- a/.gitignore[m
[1m+++ b/.gitignore[m
[36m@@ -1,4 +1,5 @@[m
 dags/data/raw/*[m
 dags/data/cleaned/*[m
 dags/data/enriched*[m
[31m-dags/reports/*[m
\ No newline at end of file[m
[32m+[m[32mdags/reports/*[m
[32m+[m[32mdags/deduplicated/*[m
\ No newline at end of file[m
[1mdiff --git a/dags/__pycache__/immo_dag.cpython-311.pyc b/dags/__pycache__/immo_dag.cpython-311.pyc[m
[1mindex 15a7ecb..a3c6cf5 100644[m
Binary files a/dags/__pycache__/immo_dag.cpython-311.pyc and b/dags/__pycache__/immo_dag.cpython-311.pyc differ
[1mdiff --git a/dags/immo_dag.py b/dags/immo_dag.py[m
[1mindex 289d029..d1ff059 100644[m
[1m--- a/dags/immo_dag.py[m
[1m+++ b/dags/immo_dag.py[m
[36m@@ -15,6 +15,7 @@[m [mfrom include.extract_data import extract_immotop_lu_data, extract_athome_data[m
 from include.data_cleaning import immotop_lu_data_cleaning, athome_lu_data_cleaning[m
 from include.data_enrichment import immotop_lu_enrichment, athome_lu_enrichment[m
 from include.reports import generate_report[m
[32m+[m[32mfrom include.duplicates_treatment import merge_all_df_and_treat_duplicates[m
 [m
 default_args = {[m
     "owner" : "airflow",[m
[36m@@ -64,6 +65,11 @@[m [mwith DAG([m
         python_callable=athome_lu_enrichment[m
     )[m
 [m
[32m+[m[32m    treat_duplicates = PythonOperator([m
[32m+[m[32m        task_id = "merge_all_df_and_treat_duplicates",[m
[32m+[m[32m        python_callable=merge_all_df_and_treat_duplicates[m
[32m+[m[32m    )[m
[32m+[m
     gen_report = PythonOperator([m
         task_id = "generate_report",[m
         python_callable=generate_report,[m
[36m@@ -73,4 +79,7 @@[m [mwith DAG([m
     extract_data_from_athome_lu >> transform_data_from_athome_lu >> athome_lu_data_enrichment[m
 [m
     athome_lu_data_enrichment >> gen_report[m
[31m-    immotop_lu_data_enrichment >> gen_report[m
\ No newline at end of file[m
[32m+[m[32m    immotop_lu_data_enrichment >> gen_report[m
[32m+[m
[32m+[m[32m    athome_lu_data_enrichment >> treat_duplicates[m
[32m+[m[32m    immotop_lu_data_enrichment >> treat_duplicates[m
\ No newline at end of file[m
[1mdiff --git a/include/__pycache__/data_cleaning.cpython-311.pyc b/include/__pycache__/data_cleaning.cpython-311.pyc[m
[1mindex 90c0290..3444d94 100644[m
Binary files a/include/__pycache__/data_cleaning.cpython-311.pyc and b/include/__pycache__/data_cleaning.cpython-311.pyc differ
[1mdiff --git a/include/__pycache__/data_enrichment.cpython-311.pyc b/include/__pycache__/data_enrichment.cpython-311.pyc[m
[1mindex 5525e78..67c988f 100644[m
Binary files a/include/__pycache__/data_enrichment.cpython-311.pyc and b/include/__pycache__/data_enrichment.cpython-311.pyc differ
[1mdiff --git a/include/__pycache__/extract_data.cpython-311.pyc b/include/__pycache__/extract_data.cpython-311.pyc[m
[1mindex 903627d..e5978df 100644[m
Binary files a/include/__pycache__/extract_data.cpython-311.pyc and b/include/__pycache__/extract_data.cpython-311.pyc differ
[1mdiff --git a/include/data_cleaning.py b/include/data_cleaning.py[m
[1mindex 4e727dd..71a5538 100644[m
[1m--- a/include/data_cleaning.py[m
[1m+++ b/include/data_cleaning.py[m
[36m@@ -1,13 +1,8 @@[m
[31m-import urllib.request[m
 import pandas as pd[m
 from datetime import date[m
 import re[m
 import os[m
[31m-import cv2[m
[31m-import numpy as np[m
 import logging[m
[31m-import sys[m
[31m-import matplotlib.pyplot as plt[m
 [m
 ##REGEX[m
 deposit_amount_reg = re.compile("((?<=Deposit amount: )€?\d+(?:\.\d+)?)|((?<=Montant caution: )€?\d+(?:\.\d+)?)", re.IGNORECASE)[m
[36m@@ -64,98 +59,6 @@[m [mdef get_district(district):[m
     else:[m
         return district.replace("Localité", "")[m
 [m
[31m-def sift_similarity(img1, img2):[m
[31m-    nfeatures = 500[m
[31m-    sift = cv2.SIFT_create(nfeatures=nfeatures)[m
[31m-[m
[31m-    #Image preprocessing[m
[31m-    img1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)[m
[31m-    img2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)[m
[31m-[m
[31m-    kp_img1, desc_img1 = sift.detectAndCompute(img1, None)[m
[31m-    kp_img2, desc_img2 = sift.detectAndCompute(img2, None)[m
[31m-[m
[31m-    index_params = dict(algorithm=1, trees=5)  # K-D Tree (algorithm=1). Increase "trees" value to improve precision, decrease to improve speed[m
[31m-    search_params = dict(checks=50)  #Amount of comparison. Increase to improve precision, decrease to improve speed[m
[31m-[m
[31m-    flann = cv2.FlannBasedMatcher(index_params, search_params)[m
[31m-[m
[31m-    # bf = cv2.BFMatcher(cv2.NORM_L2)[m
[31m-    matches = flann.knnMatch(desc_img1, desc_img2, k=2)[m
[31m-    good_matches = [m for m, n in matches if m.distance < 0.3 * n.distance][m
[31m-[m
[31m-    if len(matches) == 0:[m
[31m-        return 0[m
[31m-    return len(good_matches) / nfeatures[m
[31m-    [m
[31m-def treat_athome_duplicates(df):[m
[31m-    df = df.sort_values(by=["Price", "Surface"]).reset_index(drop=True)[m
[31m-[m
[31m-    i = 0[m
[31m-    df_len = len(df)[m
[31m-[m
[31m-    logging.info("Athome duplicates treatment")[m
[31m-[m
[31m-    #Using a while in order to change the incrementation value[m
[31m-    while i < df_len:[m
[31m-        #The count of duplicated elements compared to i[m
[31m-        duplicates_count = 0[m
[31m-[m
[31m-        #If no images to compare then skip[m
[31m-        if pd.isna(df.loc[i, "Photos"]):[m
[31m-            i += 1[m
[31m-            continue[m
[31m-[m
[31m-        for j in range (i+1, df_len):[m
[31m-            surface_diff = df.loc[j, "Surface"] - df.loc[i, "Surface"][m
[31m-            surface_diff_threshold = 5[m
[31m-[m
[31m-            if df.loc[i, "Price"] != df.loc[j, "Price"] or surface_diff > surface_diff_threshold:[m
[31m-                #Skip duplicates[m
[31m-                if duplicates_count > 0 and (j - i) == duplicates_count + 1:[m
[31m-                    i = j - 1[m
[31m-                break[m
[31m-[m
[31m-            if pd.isna(df.loc[j, "Photos"]):[m
[31m-                continue[m
[31m-[m
[31m-            logging.info(f"Comparison between accomodation line {i+2} and accomodation line {j+2}")[m
[31m-[m
[31m-            i_photos_url = df.loc[i, "Photos"].split(" ")[m
[31m-            j_photos_url = df.loc[j, "Photos"].split(" ")[m
[31m-[m
[31m-            #Initialization of variables so they are accessible in the external for loops[m
[31m-            metric_val = 0[m
[31m-            exactness_threshold = 0.90[m
[31m-[m
[31m-            for i_photo_url in i_photos_url:[m
[31m-                for j_photo_url in j_photos_url:[m
[31m-                    #Get the images from url[m
[31m-                    i_photo_request = urllib.request.urlopen(i_photo_url)[m
[31m-                    i_photo = cv2.imdecode(np.asarray(bytearray(i_photo_request.read()), dtype=np.uint8), -1)[m
[31m-[m
[31m-                    j_photo_request = urllib.request.urlopen(j_photo_url)[m
[31m-                    j_photo = cv2.imdecode(np.asarray(bytearray(j_photo_request.read()), dtype=np.uint8), -1)[m
[31m-[m
[31m-                    metric_val = sift_similarity(i_photo, j_photo)[m
[31m-                    logging.info(f"\tSIFT similarity score between {i_photo_url} and {j_photo_url} = {round(metric_val, 3)}")[m
[31m-[m
[31m-                    if metric_val >= exactness_threshold:[m
[31m-                        duplicates_count += 1[m
[31m-                        df.loc[j, "Duplicate_rank"] = duplicates_count + 1[m
[31m-[m
[31m-                        break[m
[31m-                [m
[31m-                if metric_val >= exactness_threshold:[m
[31m-                    break[m
[31m-            [m
[31m-            #Allow to skip the series of adjacent duplicates[m
[31m-            if duplicates_count > 0 and (j - i) == duplicates_count + 1:[m
[31m-                i = j - 1[m
[31m-[m
[31m-        i+=1[m
[31m-    return df[m
[31m-[m
 def immotop_lu_data_cleaning():[m
     today = str(date.today())[m
     airflow_home = os.environ["AIRFLOW_HOME"][m
[36m@@ -176,8 +79,6 @@[m [mdef immotop_lu_data_cleaning():[m
         "Condominium fees" : "Condominium_fees"[m
     }[m
 [m
[31m-    df.drop_duplicates(subset=["Link"], inplace=True)[m
[31m-[m
     df.rename(columns=column_names, inplace=True)[m
 [m
     df["Surface"] = df["Surface"].apply(lambda surface: surface.replace("m²", "").replace(" ", "") if pd.notnull(surface) else surface)[m
[36m@@ -202,23 +103,34 @@[m [mdef immotop_lu_data_cleaning():[m
     df["Floor_number"] = df["Floor_number"].apply(lambda floor_number: get_floor_number(floor_number) if pd.notna(floor_number) else floor_number)[m
     df["District"] = df["District"].apply(lambda district: get_district(district) if pd.notna(district) else district)[m
     df["Is_furnished"] = df["Is_furnished"].map({"Yes" : "Oui", "No" : "Non", "Only Kitchen Furnished" : "Non", "Partially furnished" : "Oui"})[m
[31m-    df["Heating"] = df["Heating"].map({[m
[32m+[m[32m    df["Heating"] = df["Heating"].replace({[m
         "Independent, powered by heat pump" : "Heat pump",[m
         "Independent, gas powered" : "Gas",[m
         "Independent, power supply" : "Electric",[m
         "Independent, powered by gas oil" : "Gasoil",[m
         "Independent, powered by pellets" : "Pellets"[m
     })[m
[32m+[m
[32m+[m[32m    #Use replace instead of map to avoid replacing values not present in the dict by NaN[m
[32m+[m[32m    df["District"] = df["District"].replace({[m
[32m+[m[32m        "Gasperich-Cloche d’or" : "Gasperich",[m
[32m+[m[32m        "Bonnevoie-Verlorenkost" : "Bonnevoie"[m
[32m+[m[32m    })[m
     [m
     df.drop(df[df.Type == "Building"].index, inplace=True)[m
     df.drop(columns=["Rental guarantee", "Condominium_fees"], inplace=True)[m
     df.dropna(subset=["Surface", "Price"], inplace=True)[m
 [m
[32m+[m[32m    lines_before_duplicates_removal = len(df)[m
[32m+[m[32m    df.drop_duplicates(subset=["Link"], inplace=True)[m
[32m+[m[32m    lines_after_duplicates_removal = len(df)[m
[32m+[m
[32m+[m[32m    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")[m
[32m+[m
     df.to_csv(f"{airflow_home}/dags/data/cleaned/immotop_lu_{today}.csv", index=False)[m
 [m
 def athome_lu_data_cleaning():[m
[31m-    #today = str(date.today())[m
[31m-    today = '2024-12-14'[m
[32m+[m[32m    today = str(date.today())[m
     airflow_home = os.environ["AIRFLOW_HOME"][m
 [m
     df = pd.read_csv([m
[36m@@ -231,24 +143,19 @@[m [mdef athome_lu_data_cleaning():[m
             "Bathroom" : "Int64",[m
             "Garages" : "Int64"})[m
 [m
[32m+[m[32m    df["Heating"] = df.apply(get_heating_athome, axis=1)[m
[32m+[m[32m    df["City"] = df["City"].apply(lambda city : city.strip())[m
[32m+[m[32m    df.drop(columns=["Has_electric_heating", "Has_gas_heating"], inplace=True)[m
[32m+[m
     lines_before_duplicates_removal = len(df)[m
 [m
     #Drop duplicated rows[m
     df = df.drop_duplicates(subset=["Link"])[m
[31m-[m
     lines_after_duplicates_removal = len(df)[m
[31m-[m
[31m-    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")[m
[31m-[m
[31m-    #Introduction of a new column that will be used to identify the duplicates later on[m
[31m-    #1 is the default value (= no other duplicates)[m
[31m-    df["Duplicate_rank"] = 1[m
     [m
[31m-    df = treat_athome_duplicates(df)[m
[31m-[m
[31m-    df["Heating"] = df.apply(get_heating_athome, axis=1)[m
[31m-    df.drop(columns=["Has_electric_heating", "Has_gas_heating"], inplace=True)[m
[32m+[m[32m    logging.info(f"{lines_before_duplicates_removal - lines_after_duplicates_removal} duplicates have been removed")[m
 [m
     df.to_csv(f"{airflow_home}/dags/data/cleaned/athome_last3d_{today}.csv", index=False)[m
 [m
[31m-# athome_lu_data_cleaning()[m
\ No newline at end of file[m
[32m+[m[32m# athome_lu_data_cleaning()[m
[32m+[m[32m# immotop_lu_data_cleaning()[m
\ No newline at end of file[m
[1mdiff --git a/include/data_enrichment.py b/include/data_enrichment.py[m
[1mindex 3bebdbc..e58d5e7 100644[m
[1m--- a/include/data_enrichment.py[m
[1m+++ b/include/data_enrichment.py[m
[36m@@ -172,4 +172,5 @@[m [mdef athome_lu_enrichment():[m
 [m
     df.to_csv(f"{airflow_home}/dags/data/enriched/athome_last3d_{today}.csv", index=False)[m
 [m
[31m-# immotop_lu_enrichment()[m
\ No newline at end of file[m
[32m+[m[32m# immotop_lu_enrichment()[m
[32m+[m[32m# athome_lu_enrichment()[m
\ No newline at end of file[m
[1mdiff --git a/include/extract_data.py b/include/extract_data.py[m
[1mindex 7ba5664..d9a4431 100644[m
[1m--- a/include/extract_data.py[m
[1m+++ b/include/extract_data.py[m
[36m@@ -424,7 +424,10 @@[m [mdef extract_immotop_lu_data():[m
                 slideshow_items = details.find_all("div", "nd-slideshow__item")[m
                 [m
                 for slideshow_item in slideshow_items:[m
[31m-                    item["Photos"] += slideshow_item.find("img").get("src") + " "[m
[32m+[m[32m                    img_url = slideshow_item.find("img").get("src")[m
[32m+[m[32m                    #Make sure that I don't include two times the same image in the df[m
[32m+[m[32m                    if not img_url in item["Photos"]:[m
[32m+[m[32m                        item["Photos"] += img_url + " "[m
                 [m
                 item["Photos"] = item["Photos"].rstrip()[m
             [m
