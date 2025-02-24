import logging
import os
from airflow.models import Variable

#Custom modules
from . import utils
# from utils import fetch_url_with_retries

def save_athome_df(accomodations, ds, part_number):
    import pandas as pd
    df = pd.DataFrame(accomodations)
    df["Snapshot_day"] = ds
    df["Website"] = "athome"

    parent_folder_path = f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}"
    
    if not os.path.exists(parent_folder_path):
        os.makedirs(parent_folder_path)
    df.to_csv(f"{parent_folder_path}/part_{part_number}.csv", index=False)

def extract_athome_data(part_number, total_parts, ds):
    #Import here to optimize the DAG preprocessing
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException

    import pandas as pd
    from bs4 import BeautifulSoup

    translate_table_price = str.maketrans("", "", "€ \u202f\xa0'")
    if Variable.get("immo_lux_base_url_athome_lu") == None:
        Variable.set("immo_lux_base_url_athome_lu", "https://www.athome.lu/en/srp/?tr=rent&sort=price_asc&q=faee1a4a&loc=L2-luxembourg&ptypes=house,flat&page=")

    ###CHROME OPTIONS SETUP####
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-crash-reporter")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-default-apps")
    chrome_options.add_argument("--disable-client-side-phishing-detection")
    chrome_options.add_argument("--disable-application-cache")
    chrome_options.add_argument("--incognito")
    #Define a user agent to avoid anti-bot detection
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36')
    # adding argument to disable the AutomationControlled flag
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # exclude the collection of enable-automation switches 
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # turn-off userAutomationExtension 
    chrome_options.add_experimental_option("useAutomationExtension", False)

    with webdriver.Remote(command_executor="http://localhost:4444", options=chrome_options) as driver:
        # changing the property of the navigator value for webdriver to undefined 
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.maximize_window()
        driver.implicitly_wait(10)

        ###Global variables used by BS4
        current_page = 1
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}

        page = utils.fetch_url_with_retries(Variable.get("immo_lux_base_url_athome_lu") + str(current_page), headers=headers)
        s = BeautifulSoup (page.text, "html.parser")

        max_page = int(s.find("a", "page last").get_text())

        #Calculation of starting and ending page scraping limits
        #The page at which the scraping will start
        current_page = (part_number - 1) * (max_page // total_parts) + 1
        remainder = max_page % total_parts

        if part_number != 1 and part_number - 1 <= remainder:
                current_page += 1

        max_page = current_page + (max_page // total_parts) - 1
        if part_number <= remainder:
            max_page += 1

        accomodations = []
        proceed = True

        #Retrieve the data already scraped if the task previously failed
        if os.path.exists(f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}/part_{part_number}.csv"):
            logging.info(f"Resuming the scraping of athome.lu at page {current_page} !")
            df = pd.read_csv(f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}/part_{part_number}.csv")
            current_page = len(df) // 20 + 1
            accomodations = df.to_dict("records")

        logging.info("Scraping of athome.lu has started !")
        while proceed:
            current_url = Variable.get("immo_lux_base_url_athome_lu") + str(current_page)
            page = utils.fetch_url_with_retries(current_url, headers=headers)

            s = BeautifulSoup (page.text, "html.parser")

            #Check if we have reached the end of the results
            if max_page == current_page:
                proceed = False
            else:
                #List all the properties to treat
                properties = s.find_all("article", class_= ["property-article red", "property-article standard", "property-article silver", "property-article gold", "property-article platinum"])

                for i in range(len(properties)):
                    item = {}
                    
                    #Global characteristics (optionnals such as the price, the number of rooms, etc ...)
                    characterstics = properties[i].find("ul", class_="property-card-info-icons property-characterstics")
                    surface = characterstics.find("li", class_="item-surface")
                    href = properties[i].find("a", class_="property-card-link property-title").attrs["href"]
                    
                    #Ensure that every property to include possess a surface
                    if surface != None:
                        item["Price"] = properties[i].find("span", class_="font-semibold whitespace-nowrap").get_text().translate(translate_table_price).replace(",", "")
                        item["Surface"] = surface.get_text().replace("m²", "").strip()
                        item["City"] = properties[i].find("span", class_="property-card-immotype-location-city").get_text()
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
                        
                        try:
                            #Go in the accomodation page to get additionnal informations
                            driver.get(item["Link"])

                            WebDriverWait(driver, 2.5)
                            max_waiting_time = 10
                            
                            try:
                                WebDriverWait(driver, max_waiting_time).until(EC.presence_of_element_located((By.CLASS_NAME, "showHideDesktopGallery")))
                            except TimeoutException:
                                driver.get(item["Link"])
                                WebDriverWait(driver, max_waiting_time).until(EC.presence_of_element_located((By.CLASS_NAME, "showHideDesktopGallery")))
                    
                            WebDriverWait(driver, max_waiting_time).until(EC.presence_of_all_elements_located((By.TAG_NAME, "img")))
                        except Exception as e:
                            logging.error(f"An error occured while trying to fetch the page {item['Link']} : {str(e)}")

                            if current_page != 1:
                                #current_page * 20 because each page contains 20 accomodations
                                accomodations = accomodations[:(current_page-1) * 20]
                                save_athome_df(accomodations, ds, part_number)

                        html = driver.page_source
                        details = BeautifulSoup(html, "html.parser")

                        adress_div = details.find("div", "block-localisation-address")
                        if adress_div != None:
                            full_adress = adress_div.getText()
                            
                            if full_adress.count(",") >= 2:
                                item["Address"] = full_adress.strip()
                        
                        description = details.find("div", "collapsed")
                        
                        if description != None:
                            item["Description"] = description.find("p").get_text()
                        else:
                            item["Description"] = None

                        #Type of accomodation
                        title = details.find("span", "property-card-immotype-title")
                        item["Type"] = title.find_all("span")[0].get_text()

                        monthly_charges = details.find("div", "characteristics-item charges")
                        if monthly_charges != None:
                            item["Monthly_charges"] = monthly_charges.find("span", "characteristics-item-value").get_text().translate(translate_table_price).strip()
                        else:
                            item["Monthly_charges"] = None

                        deposit = details.find("div", "characteristics-item deposit")
                        if deposit != None:
                            item["Deposit"] = deposit.find("span", "characteristics-item-value").get_text().translate(translate_table_price)
                        else:
                            item["Deposit"] = None

                        agency_fees = details.find("div", "characteristics-item agencyFees")
                        if agency_fees != None:
                            item["Agency_fees"] = agency_fees.find("span", "characteristics-item-value").get_text().translate(translate_table_price).replace(",", ".")
                        else:
                            item["Agency_fees"] = None

                        floor_number = details.find("div", "characteristics-item address.floor")
                        if floor_number != None:
                            item["Floor_number"] = floor_number.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Floor_number"] = None

                        bedrooms = details.find("div", "characteristics-item characteristic.bedrooms")
                        if bedrooms != None:
                            item["Bedrooms"] = bedrooms.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Bedrooms"] = None

                        showers = details.find("div", "characteristics-item characteristic.showers")
                        if showers != None:
                            item["Shower_room"] = showers.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Shower_room"] = None

                        bathroom = details.find("div", "characteristics-item characteristic.bathrooms")
                        if bathroom != None:
                            item["Bathroom"] = bathroom.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Bathroom"] = None
                        
                        garages = details.find("div", "characteristics-item characteristic.garages")
                        if garages != None:
                            item["Garages"] = garages.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Garages"] = None

                        is_furnished = details.find("div", "characteristics-item characteristic.isFurnished")
                        if is_furnished != None:
                            item["Is_furnished"] = is_furnished.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Is_furnished"] = None

                        has_equiped_kitchen = details.find("div", "characteristics-item characteristic.hasEquippedKitchen")
                        if has_equiped_kitchen != None:
                            item["Has_equiped_kitchen"] = has_equiped_kitchen.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_equiped_kitchen"] = None

                        has_lift = details.find("div", "characteristics-item characteristic.hasLift")
                        if has_lift != None:
                            item["Has_lift"] = has_lift.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_lift"] = None

                        has_balcony = details.find("div", "characteristics-item characteristic.hasBalcony")
                        if has_balcony != None:
                            item["Has_balcony"] = has_balcony.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_balcony"] = None

                        balcony_surface = details.find("div", "characteristics-item characteristic.balconySurface")
                        if balcony_surface != None:
                            item["Balcony_surface"] = balcony_surface.find("span", "characteristics-item-value").get_text().replace("m²", "").replace(",", ".").strip()
                        else:
                            item["Balcony_surface"] = None

                        has_terrace = details.find("div", "characteristics-item characteristic.hasTerrace")
                        if has_terrace != None:
                            item["Has_terrace"] = has_terrace.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_terrace"] = None

                        terrace_surface = details.find("div", "characteristics-item characteristic.terraceSurface")
                        if terrace_surface != None:
                            item["Terrace_surface"] = terrace_surface.find("span", "characteristics-item-value").get_text().replace("m²", "").replace(",", ".").strip()
                        else:
                            item["Terrace_surface"] = None

                        has_garden = details.find("div", "characteristics-item characteristic.hasGarden")
                        if has_garden != None:
                            item["Has_garden"] = has_garden.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_garden"] = None
                        
                        garden_surface = details.find("div", "characteristics-item characteristic.gardenSurface")
                        if garden_surface != None:
                            item["Garden_surface"] = garden_surface.find("span", "characteristics-item-value").get_text().replace("m²", "").strip()
                        else:
                            item["Garden_surface"] = None

                        has_cellar = details.find("div", "characteristics-item characteristic.hasCellar")
                        if has_cellar != None:
                            item["Has_cellar"] = has_cellar.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_cellar"] = None
                        
                        #Heating types
                        has_gas_heating = details.find("div", "characteristics-item energy.hasGasHeating")
                        if has_gas_heating != None:
                            item["Has_gas_heating"] = has_gas_heating.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_gas_heating"] = None

                        has_electric_heating = details.find("div", "characteristics-item energy.hasElectricHeating")
                        if has_electric_heating != None:
                            item["Has_electric_heating"] = has_electric_heating.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_electric_heating"] = None
                        
                        exposition = details.find("div", "characteristics-item characteristic.exposition")
                        if exposition != None:
                            item["Exposition"] = exposition.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Exposition"] = None

                        energy_class_div = details.find("div", "characteristics-item energy.energyEfficiency.energyClass")
                        if energy_class_div != None:
                            energy_class = energy_class_div.find("div", "energy-class-label")
                            if energy_class != None:
                                item["Energy_class"] = energy_class.get_text()
                            else:
                                item["Energy_class"] = None

                            insulation_class = details.find("div", "characteristics-item energy.thermalInsulation.insulationClass").find("div", "energy-class-label")
                            if insulation_class != None:
                                item["Insulation_class"] = insulation_class.get_text()
                            else:
                                item["Insulation_class"] = None

                        agency = details.find("div", class_="agency-details__name agency-details__name--centered")
                        if agency != None:
                            item["Agency"] = agency.get_text()
                        else:
                            item["Agency"] = None

                        #Add the photos of the accomodation to the dataframe
                        item["Photos"] = ""

                        desktop_gallery = details.find("div", "showHideDesktopGallery")

                        pictures = desktop_gallery.find_all("picture")
                        if len(pictures) == 0:
                            logging.info("\t\tNo pictures found")
                        elif len(pictures) > 1:
                            #Remove the last <picture> element because it doesn't contain any image
                            pictures = pictures[:-1]
                        
                        for picture in pictures:
                            img = picture.find("img")
                            item["Photos"] += img.get("src") + " "

                        #Remove the last space delimiter at the end of the string
                        item["Photos"] = item["Photos"].rstrip()
                        
                        accomodations.append(item)
                logging.info("Page " + str(current_page) + " of athome.lu has entirely been scrapped !")
                current_page+=1
    
    #Persistance of data
    save_athome_df(accomodations, ds, part_number)
    logging.info("Scraping of athome.lu successfully ran !")

def merge_athome_raw_data_parts(ds):
    import pandas as pd
    import os
    import shutil

    dfs = []
    folder = f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}"

    for file in os.listdir(folder):
        dfs.append(pd.read_csv(f"{folder}/{file}"))

    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/raw/athome_last3d_{ds}.csv", index=False)

    #Remove the temporary folder and all its content
    shutil.rmtree(folder)

def extract_immotop_lu_data(ds):
    #Import here to optimize the DAG preprocessing
    import pandas as pd
    from bs4 import BeautifulSoup

    accomodations = []

    proceed = True
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
    while proceed:
        page = utils.fetch_url_with_retries("https://www.immotop.lu/en/location-maisons-appartements/luxembourg-pays/?criterio=prezzo&ordine=asc&pag=" + str(current_page), headers=headers)
        s = BeautifulSoup (page.text, "html.parser")

        if s.find("div", "in-errorPage__bg in-errorPage__bg--generic in-errorPage__container") != None:
            proceed = False
        else:
            properties = s.find("ul", "nd-list in-searchLayoutList ls-results").find_all("li", "nd-list__item in-searchLayoutListItem")

            for i in range(len(properties)):
                item = {}

                listing_card_title = properties[i].find("a", "in-listingCardTitle")
                if listing_card_title != None:
                    item["Link"] = listing_card_title.attrs["href"]
                else:
                    continue

                #Go in the detail page to get additionnal informations
                page = utils.fetch_url_with_retries(item["Link"], headers=headers)
                details = BeautifulSoup(page.text, "html.parser")

                title = details.find("h1", "re-title__title")
                #If title is None then the page contains no other data so we skip it
                if title == None:
                    continue

                logging.info(f"\tAccomodation N°{i+1} - Scraping of accomodation with url : {item['Link']}")
                
                read_all = details.find("div", "in-readAll in-readAll--lessContent")
                if read_all != None:
                    item["Description"] = read_all.find("div").get_text()

                if "flat" in title.get_text().lower():
                    item["Is_flat"] = "Oui"

                title_parts = title.get_text().split(", ")
                title_parts_size = len(title_parts)
                item["City"] = title_parts[title_parts_size - 1]
                
                if title_parts_size > 2:
                    item["District"] = title_parts[title_parts_size - 2].replace("Localité", "")

                #To get the address
                location_spans = details.find_all("span", "re-blockTitle__location")
                if len(location_spans) == 2:
                    item["Address"] = location_spans[1].get_text()

                #Features treatment
                features = details.find_all("div", "re-featuresItem")
                for feature in features:
                    feature_title = feature.find("dt", "re-featuresItem__title").get_text()

                    if feature_title not in features_blacklist:
                        item[feature_title] = feature.find("dd", "re-featuresItem__description").get_text()
                
                energy_class = details.find("span", "re-mainConsumptions__energyCustomColor")
                if energy_class != None:
                    item["Energy_class"] = energy_class.get_text()
                else:
                    item["Energy_class"] = None

                agency_frame = details.find("div", "in-referent in-referent__withPhone")
                
                if agency_frame != None:
                    agency_p = agency_frame.find("p")
                    if agency_p != None:
                        item["Agency"] = agency_p.get_text()

                item["Photos"] = ""

                first_img_url = details.find("img", "nd-figure__content nd-ratio__img").get("src")
                if not first_img_url.startswith("/_next/"):
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
    
    #Persistance of data
    df = pd.DataFrame(accomodations)
    df["Snapshot_day"] = ds
    df["Website"] = "immotop.lu"
    df.to_csv(f"{Variable.get('immo_lux_data_folder')}/raw/immotop_lu_{ds}.csv", index=False)

    logging.info("Scraping of immotop.lu is successfully finished !")

# extract_athome_data(1, 2, date.today())