from datetime import date
import logging
import os
import utils

def extract_athome_data():
    #Import here to optimize the DAG preprocessing
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException

    import requests
    import pandas as pd
    from bs4 import BeautifulSoup

    accomodations = []

    proceed = True
    current_page = 1
    translate_table_price = str.maketrans("", "", "€ \u202f\xa0'")
    excluded_categories = ("garage-parking", "office", "commercial-property")

    ###SELENIUM SETUP####

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
    
    #Define a user agent to avoid anti-bot detection
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36')

    # adding argument to disable the AutomationControlled flag
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    # exclude the collection of enable-automation switches 
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    # turn-off userAutomationExtension 
    chrome_options.add_experimental_option("useAutomationExtension", False)

    driver_location = "/usr/bin/chromedriver"
    binary_location = "/usr/bin/google-chrome"

    chrome_options.binary_location = binary_location
    service = Service(executable_path=driver_location)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    #To change the user agents
    # selenium_user_agent = [
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    #     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    # ]
    
    # changing the property of the navigator value for webdriver to undefined 
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.maximize_window()

    #Only for selenium purpose
    first_session = True

    #Modify the user agent of BS4 to not be detected as a bot
    headers = {"user-agent" : "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}

    logging.info("Scraping of athome.lu has started !")

    while proceed:
        current_url = 'https://www.athome.lu/en/srp/?tr=rent&sort=price_asc&recent_published=3&q=faee1a4a&loc=L2-luxembourg&page=' + str(current_page)
        page = requests.get(current_url, headers=headers)

        s = BeautifulSoup (page.text, "html.parser")

        #Check if we have reached the end of the results
        if s.select_one("p.no_results") != None:
            proceed = False
        else:
            #List all the properties to treat
            properties = s.find_all("article", class_= ["property-article standard", "property-article silver", "property-article gold", "property-article platinum"])

            for i in range(len(properties)):
                item = {}
                
                #Global characteristics (optionnals such as the price, the number of rooms, etc ...)
                characterstics = properties[i].find("ul", class_="property-card-info-icons property-characterstics")
                surface = characterstics.find("li", class_="item-surface")
                href = properties[i].find("a", class_="property-card-link property-title").attrs["href"]
                
                #Ensure that every property to include possess a surface and are not categorized as garage / parking or office
                if surface != None and all(excluded_category not in href for excluded_category in excluded_categories):
                    item["Price"] = properties[i].find("span", class_="font-semibold whitespace-nowrap").get_text().translate(translate_table_price).replace(",", "")
                    item["Surface"] = surface.get_text().replace("m²", "").strip()
                    item["City"] = properties[i].find("span", class_="property-card-immotype-location-city").get_text()
                    item["Link"] = "https://www.athome.lu" + href

                    logging.info(f"\tAccomodation N°{i+1} - Scraping of accomodation with url : {item['Link']}")

                    if "apartment" in item["Link"]:
                        item["Type"] = "Apartment"
                    elif "house" in item["Link"]:
                        item["Type"] = "House"
                    else:
                        item["Type"] = None

                    #Get the district (only for Luxembourg City)
                    if item["City"].strip().startswith("Luxembourg"):
                        splitted_str = item["City"].split("-")
                        
                        item["City"] = splitted_str[0]

                        #Very rare case
                        if len(splitted_str) > 1:
                            item["District"] = splitted_str[1]

                    #Go in the detail page to get additionnal informations
                    page = utils.fetch_url_with_retries(item["Link"], headers=headers)
                    details = BeautifulSoup(page.text, "html.parser")
                    
                    description = details.find("div", "collapsed")
                    
                    if description != None:
                        item["Description"] = description.find("p").get_text()
                    else:
                        item["Description"] = None

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

                    bathroom = details.find("div", "characteristics-item characteristic.showers")
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
                    
                    adress_div = details.find("div", "block-localisation-address")
                    if adress_div != None:
                        full_adress = adress_div.getText()

                        #For debugging purpose only
                        print(full_adress)
                        
                        if full_adress.count(",") >= 2:
                            item["Adress"] = full_adress

                    agency = details.find("div", class_="agency-details__name agency-details__name--centered")
                    if agency != None:
                        item["Agency"] = agency.get_text()
                    else:
                        item["Agency"] = None

                    #Add the photos of the accomodation to the dataframe
                    item["Photos"] = ""

                    WebDriverWait(driver, 2)
                    #Because the DOM can change due to responsiveness
                    driver.get(item["Link"])

                    #Time to wait before timeout
                    max_waiting_time = 20
                    
                    #To accept the cookies the first time
                    if first_session:
                        WebDriverWait(driver, max_waiting_time).until(
                            EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler"))
                        )
                        accept_cookies = driver.find_element(By.ID, "onetrust-accept-btn-handler")
                        accept_cookies.click()

                        first_session = False
                    WebDriverWait(driver, max_waiting_time).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "showHideDesktopGallery"))
                    )
                    desktop_gallery = driver.find_element(By.CLASS_NAME, "showHideDesktopGallery")
                    ul_photos = desktop_gallery.find_element(By.TAG_NAME, "ul")
                        
                    #Find picture first instead of img to avoid the maps
                    for picture in ul_photos.find_elements(By.TAG_NAME, "picture"):
                        try:
                            img = picture.find_element(By.TAG_NAME, "img")
                            item["Photos"] += img.get_attribute("src") + " "
                        except NoSuchElementException:
                            pass
                    
                    #Remove the last space delimiter at the end of the string
                    item["Photos"] = item["Photos"].rstrip()
                    
                    accomodations.append(item)
            logging.info("Page " + str(current_page) + " of athome.lu has entirely been scrapped !")
            current_page+=1
    
    driver.quit()
    
    #Persistance of data
    df = pd.DataFrame(accomodations)
    today = str(date.today())
    airflow_home = os.environ["AIRFLOW_HOME"]
    df["Snapshot_day"] = today
    df["Website"] = "athome"
    df.to_csv(f"{airflow_home}/dags/data/raw/athome_last3d_{today}.csv", index=False)

    logging.info("Scraping of athome.lu successfully ran !")


def extract_immotop_lu_data():
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
        page = fetch_url_with_retries("https://www.immotop.lu/en/location-maisons-appartements/luxembourg-pays/?criterio=prezzo&ordine=asc&pag=" + str(current_page), headers=headers)
        s = BeautifulSoup (page.text, "html.parser")

        if s.find("div", "nd-alert nd-alert--warning in-errorMessage__alert in-errorMessage__title") != None:
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

                title_parts = title.get_text().split(", ")
                title_parts_size = len(title_parts)
                item["City"] = title_parts[title_parts_size - 1]
                
                if title_parts_size > 2:
                    item["District"] = title_parts[title_parts_size - 2].replace("Localité", "")

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
    today = str(date.today())
    airflow_home = os.environ["AIRFLOW_HOME"]
    df = pd.DataFrame(accomodations)
    df["Snapshot_day"] = today
    df["Website"] = "immotop.lu"
    df.to_csv(f"{airflow_home}/dags/data/raw/immotop_lu_{today}.csv", index=False)

    logging.info("Scraping of immotop.lu is successfully finished !")

# extract_athome_data()