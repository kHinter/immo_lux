import logging
from airflow.models import Variable
from datetime import date, timedelta

#Custom modules
from . import utils

def extract_athome_data(ds):
    #Import here to optimize the DAG preprocessing
    import re
    import pandas as pd
    from bs4 import BeautifulSoup

    yesterday = (date.today() - timedelta(days=1)).isoformat()

    if ds == yesterday:
        translate_table_price = str.maketrans("", "", "€ \u202f\xa0'")
        athome_url = "https://www.athome.lu/en/srp/?tr=rent&sort=price_asc&q=faee1a4a&loc=L2-luxembourg&ptypes=house,flat&page="

        img_suffix_reg = re.compile("(?<=content=).+", re.IGNORECASE)

        ###Global variables used by BS4
        current_page = 1
        headers = {"user-agent" : "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}
        accomodations = []
        proceed = True

        logging.info("Scraping of athome.lu has started !")
        while proceed:
            current_url = athome_url + str(current_page)
            page = utils.fetch_url_with_retries(current_url, headers=headers)

            s = BeautifulSoup (page.text, "html.parser")

            #Check if we have reached the end of the results
            if s.select_one("p.no_results") is not None:
                proceed = False
            else:
                #List all the properties to treat
                properties = s.find_all("article", class_= ["property-article red", "property-article standard", "property-article silver", "property-article gold", "property-article platinum"])

                for i in range(len(properties)):
                    item = {}
                    
                    #Global characteristics (optionnals such as the price, the number of rooms, etc ...)
                    characteristics = properties[i].find("ul", class_="property-card-info-icons property-characterstics")
                    surface = characteristics.find("li", class_="item-surface")
                    href = properties[i].find("a", class_="property-card-link property-title").attrs["href"]
                    
                    #Ensure that every property to include possess a surface
                    if surface is not None:
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

                        detail_page = utils.fetch_url_with_retries(item["Link"], headers=headers)
                        details = BeautifulSoup(detail_page.text, "html.parser")

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

                        building_total_floors = details.find("div", "characteristics-item characteristic.floors")
                        if building_total_floors != None:
                            item["Building_total_floors"] = building_total_floors.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Building_total_floors"] = None

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
                            item["Terrace_surface"] = terrace_surface.find("span", "characteristics-item-value").get_text().replace("m", "").replace("²", "").replace(",", ".").strip()
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

                        has_attic = details.find("div", "characteristics-item characteristic.hasAttic")
                        if has_attic != None:
                            item["Has_attic"] = has_attic.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_attic"] = None

                        has_swimming_pool = details.find("div", "characteristics-item characteristic.hasSwimmingPool")
                        if has_swimming_pool != None:
                            item["Has_swimming_pool"] = has_swimming_pool.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_swimming_pool"] = None
                        
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

                        has_oil_heating = details.find("div", "characteristics-item energy.hasOilHeating")
                        if has_oil_heating != None:
                            item["Has_oil_heating"] = has_oil_heating.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_oil_heating"] = None

                        has_heat_pump = details.find("div", "characteristics-item energy.hasPumpHeating")
                        if has_heat_pump != None:
                            item["Has_heat_pump"] = has_heat_pump.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_heat_pump"] = None

                        has_pellet_heating = details.find("div", "characteristics-item energy.hasPelletsHeating")
                        if has_pellet_heating != None:
                            item["Has_pellet_heating"] = has_pellet_heating.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_pellet_heating"] = None

                        has_solar_panel = details.find("div", "characteristics-item energy.hasSolarPanels")
                        if has_solar_panel != None:
                            item["Has_solar_panel"] = has_solar_panel.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Has_solar_panel"] = None

                        construction_year = details.find("div", "characteristics-item characteristic.constructionYear")
                        if construction_year != None:
                            item["Construction_year"] = construction_year.find("span", "characteristics-item-value").get_text()
                        else:
                            item["Construction_year"] = None
                        
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

                        desktop_gallery_square_divs = desktop_gallery.find_all("div", "square")
                        for square_div in desktop_gallery_square_divs:
                            anchor = square_div.find("a")

                            aria_label = anchor.get("aria-label")
                            if aria_label is not None and "photos" in aria_label:
                                match = img_suffix_reg.search(anchor.get("href"))
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

    yesterday = (date.today() - timedelta(days=1)).isoformat()

    if ds == yesterday:
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
                properties_ul = s.find("ul", "nd-list in-searchLayoutList ls-results")
                
                #Specific case : if there is a single accomodation on the page
                if properties_ul is None:
                    properties_ul = s.find("ul", "nd-list in-searchLayoutList ls-results ls-results--single")

                properties = properties_ul.find_all("li", "nd-list__item in-searchLayoutListItem")

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

                    title = details.find("h1", "ld-title__title")
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
                    location_spans = details.find_all("span", "ld-blockTitle__location")
                    if len(location_spans) == 2:
                        item["Address"] = location_spans[1].get_text()

                    #Features treatment
                    features = details.find_all("div", "ld-featuresItem")
                    for feature in features:
                        feature_title = feature.find("dt", "ld-featuresItem__title").get_text()

                        if feature_title not in features_blacklist:
                            item[feature_title] = feature.find("dd", "ld-featuresItem__description").get_text()
                    
                    energy_class = details.find("span", "ld-energyMainConsumptions__consumptionColorClass ld-energyRating")
                    if energy_class != None:
                        item["Energy_class"] = energy_class.get_text()
                    else:
                        item["Energy_class"] = None

                    insulation_class = details.find("span", "ld-energyMainConsumptions__consumptionColorClass ld-energyThermal")
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

# extract_athome_data("2025-03-03")