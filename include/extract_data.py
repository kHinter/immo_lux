import logging
from airflow.sdk import Variable
from datetime import date, timedelta
from seleniumbase import SB

def extract_immotop_lu_data(ds):
    #Import here to optimize the DAG preprocessing
    import pandas as pd

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    if ds == yesterday or ds == today:
        accomodations = []
        current_page = 1

        with SB(uc=True, incognito=True, locale="en", xvfb=True, agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36") as sb:
            sb.activate_cdp_mode("https://www.immotop.lu/")
            sb.sleep(2)
            sb.uc_gui_click_captcha()
            # sb.save_screenshot(f"immotop_lu.png")

            logging.info("Scraping of immotop.lu has started !")

            while True:
                sb.activate_cdp_mode(f"https://www.immotop.lu/en/location-maisons-appartements/luxembourg-pays/?criterio=prezzo&ordine=asc&pag={str(current_page)}")
                sb.sleep(1.5)
                sb.uc_gui_click_captcha()
                # sb.save_screenshot(f"immotop_lu_page_{str(current_page)}.png")

                properties_urls = [listing_card_title.get_attribute("href") for listing_card_title in sb.select_all("a[class='Title_title__kPgMu']")]

                for i in range(len(properties_urls)):
                    item = {}

                    item["Link"] = properties_urls[i]

                    sb.sleep(1.2)

                    #Navigate to the details page of the accomodation
                    sb.goto(item["Link"])

                    title = sb.locator("h1")
                    #If title don't exists, then we skip the line
                    if title is None:
                        continue

                    item["Title"] = title.text
                    logging.info(f"\tAccomodation N°{i+1} - Scraping of accomodation with url : {item['Link']}")
                    
                    read_all = sb.select("div[class='ReadAll_readAll__nryPL ReadAll_readAll__lessContent__aOH9h']")
                    if read_all is not None:
                        item["Description"] = read_all.text_all

                    title_parts = item["Title"].split(", ")
                    title_parts_size = len(title_parts)
                    item["City"] = title_parts[title_parts_size - 1]
                    
                    if title_parts_size > 2:
                        item["District"] = title_parts[title_parts_size - 2].replace("Localité", "")

                    #To get the address
                    location_spans = sb.select_all("span[class='LocationInfo_location__JhfVr']")
                    if len(location_spans) == 2:
                        item["Address"] = location_spans[1].text

                    #Features treatment

                    sb.sleep(1.5)
                    #Access the dialog that contains all the features of the accomodation ("SEE ALL FEATURES" BUTTON)
                    sb.click("button[class='nd-button PrimaryFeatures_button__B4aSd']")

                    detailed_features_names = [feature_name.text for feature_name in sb.select_all("dt[class='DialogSection_featureTitle__I21Ax']")]
                    detailed_features_values = [feature_value.text for feature_value in sb.select_all("dd[class='DialogSection_description__FTCWE']")]

                    item.update(zip(detailed_features_names, detailed_features_values))

                    #Close the opened dialog to get back to be able to scrape the rest of the accomodation page
                    sb.sleep(1)
                    sb.click("button[class='nd-button FeaturesDialog_close__j3tj6']")

                    feature_names = [feature_name.text for feature_name in sb.select_all("dt[class='Item_title__qN4MU']")]
                    feature_values = [feature_value.text for feature_value in sb.select_all("dd[class='Item_description__nPd2L']")]

                    item.update(zip(feature_names, feature_values))

                    #Get both main consumption title and value (ex: Energy class : E) in the same list
                    main_consumption_details = [consumption.text for consumption in sb.select_all("div[class='MainConsumptions_consumptions__hW1mi'] div div")]
                    main_consumption_details_dict = dict(zip(main_consumption_details[::2], main_consumption_details[1::2]))
                    item.update(main_consumption_details_dict)

                    item["Agency_name"] = sb.locator("div[data-cy='agency-data'] p").text
                    item["Agency_page_url"] = sb.locator("div[data-cy='agency-data'] a").get_attribute("href")

                    item["Photos"] = ""
                    images = sb.select_all("img[fetchpriority]")

                    for image in images:
                        image_url = image.get_attribute("src")
                        item["Photos"] += image_url + " "
                    
                    item["Photos"] = item["Photos"].rstrip()

                    accomodations.append(item)

                logging.info("Page " + str(current_page) + " of immotop.lu has entirely been scrapped !")
                current_page += 1

        logging.info("Scraping of immotop.lu is successfully finished !")
        
        #Persistance of data
        df = pd.DataFrame(accomodations)
        df["Snapshot_day"] = ds
        df["Website"] = "immotop.lu"

        df.to_csv(f"gs://accomodations-lux/raw/immotop/immotop_lu_{ds}.csv", index=False)
    else:
        logging.error(f"The extraction task can't be executed because its execution date ({ds}) is earlier or later than yesterday ({yesterday}) !")