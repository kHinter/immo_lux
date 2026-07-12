import logging
from airflow.sdk import Variable
from datetime import date, timedelta
from playwright.sync_api import sync_playwright
from seleniumbase import SB

def extract_athome_data(ds):
    #Import here to optimize the DAG preprocessing
    import re
    import pandas as pd

    import gcsfs

    #Initialize GCS File System
    fs = gcsfs.GCSFileSystem()

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    if ds == yesterday or ds == today:
        translate_table_price = str.maketrans("", "", "€ \u202f\xa0'")
        athome_url = "https://www.athome.lu/en/srp/?tr=rent&sort=price_asc&q=faee1a4a&loc=L2-luxembourg&ptypes=house,flat&page="

        img_suffix_reg = re.compile("(?<=content=).+", re.IGNORECASE)

        current_page = 1
        accomodations = []
        proceed = True

        PARTIAL_DF_PATH = f"gs://accomodations-lux/raw/athome/athome_{ds}_partial.csv"

        if Variable.get("latest_page_scraped", default=None) is not None and fs.exists(PARTIAL_DF_PATH):
            current_page = int(Variable.get("latest_page_scraped")) + 1

            df = pd.read_csv(PARTIAL_DF_PATH)
            accomodations = df.to_dict(orient="records")

            #Remove the partial file since we will create a new one at the end of the scraping
            fs.rm(PARTIAL_DF_PATH)

            logging.info(f"Resuming the scraping of athome.lu from page {current_page} since the latest page scraped is {Variable.get('latest_page_scraped')} !")
        try:
            with sync_playwright() as p:
                logging.info("Scraping of athome.lu has started !")

                while proceed:
                    #Added the args to reduce resources usage
                    browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox"])
                    main_page = browser.new_page()
                    detail_page = browser.new_page()

                    current_url = athome_url + str(current_page)
                    main_page.goto(current_url)

                    #Check if we have reached the end of the results
                    if main_page.locator("p.no_results").is_visible():
                        proceed = False
                    else:
                        #List all the properties to treat
                        properties = main_page.locator("article[class*='property-article']")

                        for i in range(properties.count()):
                            current_property = properties.nth(i)
                            item = {}
                            
                            surface = current_property.locator("li.item-surface")
                            href = current_property.locator("link[itemprop='url']").get_attribute("href")
                            
                            #Ensure that every property to include possess a surface
                            if surface.is_visible():
                                item["Surface"] = surface.text_content().replace("m²", "").strip()
                                item["City"] = current_property.locator("span.property-card-immotype-location-city").text_content()
                                item["Link"] = "https://www.athome.lu" + href

                                logging.info(f"\tAccomodation N°{i+1} - Scraping of accomodation with url : {item['Link']}")

                                #Ignore new properties
                                if "new-property" in item["Link"]:
                                    continue

                                #Get the district (only for Luxembourg City)
                                if item["City"].strip().startswith("Luxembourg"):
                                    splitted_str = item["City"].split("-")
                                    
                                    item["City"] = splitted_str[0]

                                    #Very rare case
                                    if len(splitted_str) > 1:
                                        item["District"] = splitted_str[1]

                                detail_page.goto(item["Link"])
                                detail_page.wait_for_timeout(1000)

                                #Accept cookies to remove the banner that prevents from scraping the page
                                # detail_page.click("button#onetrust-accept-btn-handler")

                                item["Price"] = detail_page.locator("span.font-semibold.whitespace-nowrap").text_content().translate(translate_table_price).strip()

                                adress_div = detail_page.locator("div.block-localisation-address")
                                if adress_div.is_visible():
                                    full_adress = adress_div.text_content()
                                    
                                    if full_adress.count(",") >= 2:
                                        item["Address"] = full_adress.strip()
                                
                                description = detail_page.locator("div.collapsed")
                                
                                if description.is_visible():
                                    item["Description"] = description.locator("p[class='text-[#333]']").text_content()
                                else:
                                    item["Description"] = None

                                #Type of accomodation
                                title = detail_page.locator("span.property-card-immotype-title")
                                item["Type"] = title.locator("span").first.text_content().strip()

                                characteristic_blacklist = ("price", "surface")

                                #Loop to get all the characteristics of the accomodation
                                characteristics_divs = detail_page.locator("div.characteristics-item")
                                for charcharacteristics_div in characteristics_divs.all():
                                    characteristic_classes = charcharacteristics_div.get_attribute("class").split(" ")
                                    characteristic_name_splitted = characteristic_classes[len(characteristic_classes) - 1].split(".")
                                    characteristic_name = characteristic_name_splitted[len(characteristic_name_splitted) - 1]

                                    if characteristic_name not in characteristic_blacklist:
                                        characteristic_value = charcharacteristics_div.locator("span.characteristics-item-value").text_content().translate(translate_table_price).replace("m²", "").strip()

                                        if characteristic_value == "Blank":
                                            characteristic_value = None
                                        item[characteristic_name] = characteristic_value

                                agency = detail_page.locator("div.agency-details__name.agency-details__name--centered")
                                if agency.is_visible():
                                    item["Agency"] = agency.text_content()
                                else:
                                    item["Agency"] = None

                                #Add the photos of the accomodation to the dataframe
                                item["Photos"] = ""

                                desktop_gallery = detail_page.locator("div.showHideDesktopGallery")

                                desktop_gallery_square_divs = desktop_gallery.locator("div.square")
                                for square_div in desktop_gallery_square_divs.all():
                                    #To avoid adding map image to the photos list
                                    map_container_div = square_div.locator("div[class*=GalleryTheme__MapContainer]")
                                    if not map_container_div.is_visible():
                                        anchor = square_div.locator("a")

                                        aria_label = anchor.get_attribute("aria-label")
                                        if aria_label is not None and "photos" in aria_label:
                                            match = img_suffix_reg.search(anchor.get_attribute("href"))
                                            if match:
                                                image_url = "https://i1.static.athome.eu/images/annonces2/image_" + match.group()
                                                item["Photos"] += image_url + " "
                                            else:
                                                logging.warning("Image URL not found in the anchor tag !")

                                #Remove the last space delimiter at the end of the string
                                item["Photos"] = item["Photos"].rstrip()
                                accomodations.append(item)
                            else:
                                link = 'https://www.athome.lu' + href
                                logging.warning(f"No surface found for the accomodation n°{i+1} ( {link} ) !")

                        logging.info("Page " + str(current_page) + " of athome.lu has entirely been scrapped !")
                        current_page+=1
                    browser.close()
        except Exception as e:
            logging.error(f"An error occurred during the scraping of athome.lu {str(e)}")

            df = pd.DataFrame(accomodations)

            Variable.set("latest_page_scraped", str(current_page - 1))

            #TODO adapt the line below to create and store the raw data in GCS because error of lacking local permissions currentlys
            df.to_csv(f"gs://accomodations-lux/raw/athome/athome_{ds}_partial.csv", index=False)
            logging.info("Data scraped so far sucessfully saved !")

            raise RuntimeError(f"{str(e)}")

        logging.info("Scraping of athome.lu successfully ran !")

        #Persistance of data
        df = pd.DataFrame(accomodations)
        df["Snapshot_day"] = ds
        df["Website"] = "athome"

        df.to_csv(f"gs://accomodations-lux/raw/athome/athome_{ds}.csv", index=False)
    else:
        logging.error(f"The extraction task can't be executed because its execution date ({ds}) is earlier or later than yesterday ({yesterday}) !")

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