#import xml.etree.ElementTree as ET
from lxml import etree as ET #lxml turns out to be WAY faster than the built in xml parser
import pandas as pd
import os
import time

def serverName(path):
    '''Get server name from save file'''
    tree = ET.parse(path+'/Sandbox.sbc')
    root = tree.getroot()
    try:
        return root.find("SessionName").text
    except AttributeError as exp:
        print("Could not find server name in Sandbox.sbc")
        raise

def getPlayers(path):
    tree = ET.parse(path + '/Sandbox.sbc')
    root = tree.getroot()
    players = root.iter("MyObjectBuilder_Identity")
    '''Find all the IDs and players so we can change ID into player name later'''
    ids = []
    for player in players:
        ids.append(
            {
                "Name": player.find("DisplayName").text,
                "ID": player.find("IdentityId").text
            }
        )

    return pd.DataFrame(ids)


def getStore(path):
    '''Now parse the other save file for store data'''

    tree = ET.parse(path + '/SANDBOX_0_0_0_.sbs')

    start = time.time()
    root = tree.getroot()





    rows = []

    # Get all entities
    for grid in root.iter('MyObjectBuilder_EntityBase'):
        # If the entity is a grid
        if grid.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_CubeGrid":

            grid_name = grid.find("DisplayName").text  # The grid name
            # blocks = grid.find("CubeBlocks")  # The blocks in the

            stores = grid.findall("CubeBlocks[@{http://www.w3.org/2001/XMLSchema-instance}type='MyObjectBuilder_CubeGrid']")

            position_data = grid.find("PositionAndOrientation").find("Position")  # The position of the grid
            x = position_data.attrib['x']
            y = position_data.attrib['y']
            z = position_data.attrib['z']

            for store in stores:
                items = store.find("PlayerItems").findall("MyObjectBuilder_StoreItem")  # Get all the items
                owner = store.find("Owner").text  # Get the ID

                # Lookup the ID and change it to a name
                owner = ids['Name'].loc[ids['ID'] == owner].values[0]

                for item in items:


                    item_name = item.find("Item").attrib["Subtype"]  # The item name
                    item_type = item.find("StoreItemType").text  # Offer or order
                    price_per_unit = item.find("PricePerUnit").text  # Price per unit
                    quantity = item.find("Amount").text  # Qty


                    # This is a neat trick for making dataframes. It's a list of dictionaries
                    rows.append({
                        "Server": Server,
                        "Grid Name": grid_name,
                        "X": x,
                        "Y": y,
                        "Z": z,
                        "Owner": owner,
                        "Item": item_name,
                        "Offer or Order": item_type,
                        "Qty": quantity,
                        "Price per unit": price_per_unit
                    }
                    )
    print(time.time() - start)
    return rows

ls = os.listdir (".")

all_stores_data = []

for item in ls:

    if os.path.isdir(os.path.join(os.path.abspath("."), item)) and 'Expanse' in item:
        path = os.path.join(os.path.abspath("."), item)
        sub_dir = os.listdir (path)

        sub_dir_paths = []
        for dir in sub_dir:
            if os.path.isdir(os.path.join(os.path.abspath(path), dir)):
                sub_dir_paths.append(os.path.join(os.path.abspath(path), dir))
        latest_subdir = max(sub_dir_paths, key=os.path.getmtime)
        print(latest_subdir)

        Server = serverName(latest_subdir)
        ids = getPlayers(latest_subdir)
        all_stores_data.append(getStore(latest_subdir))



#Flatten the list
flat_list = [item for sublist in all_stores_data for item in sublist]


#Make a dataframe
df = pd.DataFrame(flat_list)

#Save a dataframe
df.to_csv("test.csv")

