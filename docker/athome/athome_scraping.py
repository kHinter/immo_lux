import logging
from datetime import date, timedelta
from playwright.sync_api import sync_playwright
import re
import pandas as pd
import gcsfs

last_page_scraped = 0

def extract_athome_data(ds):
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
    
        with sync_playwright() as p:
            logging.info("Scraping of athome.lu has started !")

            while proceed:
                #Added the args to reduce resources usage
                browser = p.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox"])
                main_page = browser.new_page()
                detail_page = browser.new_page()

                try:
                    current_url = athome_url + str(current_page)
                    main_page.goto(current_url)

                    #Check if we have reached the end of the results
                    if main_page.locator("p.no_results").is_visible():
                        break
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
                except Exception as e:
                    logging.error(f"An error occurred during the scraping of athome.lu, resuming extraction soon : {str(e)}")
                browser.close()
        logging.info("Scraping of athome.lu successfully ran !")

        #Persistance of data
        df = pd.DataFrame(accomodations)
        df["Snapshot_day"] = ds
        df["Website"] = "athome"

        df.to_csv(f"gs://accomodations-lux/raw/athome/athome_{ds}.csv", index=False)
    else:
        logging.error(f"The extraction task can't be executed because its execution date ({ds}) is earlier or later than yesterday ({yesterday}) !")

if __name__ == "__main__":
    extract_athome_data(date.today().isoformat())