import logging, os
from airflow.sdk import Variable
from datetime import date, timedelta

#Custom modules
# from utils import fetch_url_with_retries, create_data_related_folder_if_not_exists
from . import utils

def extract_athome_data(ds):
    #Import here to optimize the DAG preprocessing
    import re
    import pandas as pd

    from playwright.sync_api import sync_playwright

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    if ds == yesterday or ds == today:
        translate_table_price = str.maketrans("", "", "€ \u202f\xa0'")
        athome_url = "https://www.athome.lu/en/srp/?tr=rent&sort=price_asc&q=faee1a4a&loc=L2-luxembourg&ptypes=house,flat&page="

        img_suffix_reg = re.compile("(?<=content=).+", re.IGNORECASE)

        current_page = 1
        accomodations = []
        proceed = True

        if Variable.get("latest_page_scraped", default_var=None) is not None:
            current_page = int(Variable.get("latest_page_scraped")) + 1

            df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/raw/athome_{ds}_partial.csv")
            accomodations = df.to_dict(orient="records")

            #Remove the partial file since we will create a new one at the end of the scraping
            os.remove(f"{Variable.get('immo_lux_data_folder')}/raw/athome_{ds}_partial.csv")

            logging.info(f"Resuming the scraping of athome.lu from page {current_page} since the latest page scraped is {Variable.get('latest_page_scraped')} !")
        try:
            with sync_playwright() as p:
                logging.info("Scraping of athome.lu has started !")

                while proceed:
                    browser = p.chromium.launch(headless=True)
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
                                detail_page.wait_for_timeout(1500)

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
                                    item["Description"] = description.locator("p").text_content()
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

            Variable.set("latest_page_scraped", current_page - 1)

            utils.create_data_related_folder_if_not_exists("raw")
            df.to_csv(f"{Variable.get('immo_lux_data_folder')}/raw/athome_{ds}_partial.csv", index=False)
            logging.info("Data scraped so far sucessfully saved !")

        logging.info("Scraping of athome.lu successfully ran !")

        #Persistance of data
        df = pd.DataFrame(accomodations)
        df["Snapshot_day"] = ds
        df["Website"] = "athome"

        utils.create_data_related_folder_if_not_exists("raw")
        df.to_csv(f"{Variable.get('immo_lux_data_folder')}/raw/athome_{ds}.csv", index=False)
    else:
        logging.error(f"The extraction task can't be executed because its execution date ({ds}) is earlier or later than yesterday ({yesterday}) !")

def extract_immotop_lu_data(ds):
    #Import here to optimize the DAG preprocessing
    import pandas as pd
    from bs4 import BeautifulSoup
    import requests

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    if ds == yesterday or ds == today:
        accomodations = []
        current_page = 1

        #Modify the user agent to not be detected as a bot
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}

        #List of features to not include in the df
        features_blacklist = (
            "Contract", "Rooms",
            "Fees to be paid by", "Applied VAT",
            "Building floors", "Availability", 
            "Car parking", "Total building floors", 
            "Air conditioning", "Kitchen",
            "Current building use", "Price per m²")

        logging.info("Scraping of immotop.lu has started !")
        while True:
            page = requests.get(f"https://www.immotop.lu/en/location-maisons-appartements/luxembourg-pays/?criterio=prezzo&ordine=asc&pag={str(current_page)}", timeout=10, headers=headers)
            if page.status_code != 200:
                break
            s = BeautifulSoup (page.text, "html.parser")

            properties_ul = s.find("ul", "nd-list styles_in-searchLayoutList__5CPEE styles_ls-results__fY46V")

            #If there is no accomodation on the page then we stop the scraping
            if properties_ul is None:
                break
            
            #Specific case : if there is a single accomodation on the page
            if properties_ul is None:
                properties_ul = s.find("ul", "nd-list in-searchLayoutList ls-results ls-results--single")

            properties = properties_ul.find_all("li", "nd-list__item styles_in-searchLayoutListItem__y8aER")

            for i in range(len(properties)):
                item = {}

                listing_card_title = properties.nth(i).find("a", "styles_in-listingCardTitle__Wy437")
                if listing_card_title is not None:
                    item["Link"] = listing_card_title.attrs["href"]

                else:
                    continue

                #Go in the detail page to get additionnal informations
                page = utils.fetch_url_with_retries(item["Link"], headers=headers)
                details = BeautifulSoup(page.text, "html.parser")

                title = details.find("h1", "styles_ld-title__title__Ww2Gb")
                #If title is None then the page contains no other data so we skip it
                if title is None:
                    continue

                logging.info(f"\tAccomodation N°{i+1} - Scraping of accomodation with url : {item['Link']}")
                
                read_all = details.find("div", "styles_in-readAll__04LDT styles_in-readAll--lessContent__2CPCf")
                if read_all is not None:
                    item["Description"] = read_all.find("div").get_text()

                if "flat" in title.get_text().lower():
                    item["Is_flat"] = "Oui"

                title_parts = title.get_text().split(", ")
                title_parts_size = len(title_parts)
                item["City"] = title_parts[title_parts_size - 1]
                
                if title_parts_size > 2:
                    item["District"] = title_parts[title_parts_size - 2].replace("Localité", "")

                #To get the address
                location_spans = details.find_all("span", "styles_ld-blockTitle__location__n2mZJ")
                if len(location_spans) == 2:
                    item["Address"] = location_spans[1].get_text()

                #Features treatment
                features = details.find_all("div", "styles_ld-featuresItem__x3nh4")
                for feature in features:
                    feature_title = feature.find("dt", "styles_ld-featuresItem__title__SXc0E").get_text()

                    if feature_title not in features_blacklist:
                        item[feature_title] = feature.find("dd", "styles_ld-featuresItem__description__vRF1f").get_text()
                
                energy_class = details.find("span", "styles_ld-energyMainConsumptions__consumptionColorClass__o4IZg styles_ld-energyRating__aCjct")
                if energy_class != None:
                    item["Energy_class"] = energy_class.get_text()
                else:
                    item["Energy_class"] = None

                insulation_class = details.find("span", "styles_ld-energyMainConsumptions__consumptionColorClass__o4IZg styles_ld-energyThermal__nVpnn")
                if insulation_class != None:
                    item["Insulation_class"] = insulation_class.get_text()
                else:
                    item["Insulation_class"] = None

                agency_frame = details.find("div", "in-referent in-referent__withPhone")
                
                if agency_frame != None:
                    agency_p = agency_frame.find("p")
                    if agency_p != None:
                        item["Agency"] = agency_p.get_text()

                item["Photos"] = ""

                first_img_url = details.find("img", "nd-figure__content nd-ratio__img").get("src")
                #To avoid getting images not related to accomodations
                if "_next/static/media" not in first_img_url:
                    item["Photos"] += first_img_url + " "

                slideshow_items = details.find_all("div", "nd-slideshow__item")
                
                for slideshow_item in slideshow_items:
                    img_url = slideshow_item.find("img").get("src")
                    #Make sure that I don't include two times the same image in the df
                    if not img_url in item["Photos"]:
                        item["Photos"] += img_url + " "
                
                item["Photos"] = item["Photos"].rstrip()

                accomodations.append(item)

            logging.info("Page " + str(current_page) + " of immotop.lu has entirely been scrapped !")
            current_page += 1

        logging.info("Scraping of immotop.lu is successfully finished !")
        
        #Persistance of data
        df = pd.DataFrame(accomodations)
        df["Snapshot_day"] = ds
        df["Website"] = "immotop.lu"

        utils.create_data_related_folder_if_not_exists("raw")
        df.to_csv(f"{Variable.get('immo_lux_data_folder')}/raw/immotop_lu_{ds}.csv", index=False)
    else:
        logging.error(f"The extraction task can't be executed because its execution date ({ds}) is earlier or later than yesterday ({yesterday}) !")

# extract_athome_data("2026-05-25")