#import xml.etree.ElementTree as ET
from lxml import etree as ET  # lxml turns out to be WAY faster than the built in xml parser
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select, types
import time

''''Define functions to be used later:'''

def serverName(path):
    '''Get server name from save file'''
    tree = ET.parse(path + '/Sandbox.sbc')
    root = tree.getroot()
    try:
        serverName = root.find("SessionName").text

        return serverName
    except AttributeError:
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


def serverSQL(serverName, conn, metadata):
    table = metadata.tables['servers']
    exists = table.select().where(table.c.servername == serverName)
    res = conn.execute(exists).scalar()
    if res is None:
        ins = table.insert().values(servername=serverName)
        conn.execute(ins)


def getStore(path,ids):
    '''Now parse the other save file for store data'''
    namespaces = {'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
    tree = ET.parse(path + '/SANDBOX_0_0_0_.sbs')
    root = tree.getroot()
    rows = []

    # Get all entities
    for grid in root.iter('MyObjectBuilder_EntityBase'):
        # If the entity is a grid
        if grid.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_CubeGrid":

            grid_name = grid.find("DisplayName").text  # The grid name

            if "PUBLIC" in grid_name:
                stores = grid.findall(".//MyObjectBuilder_CubeBlock[@xsi:type='MyObjectBuilder_StoreBlock']", namespaces)

                position_data = grid.find("PositionAndOrientation").find("Position")  # The position of the grid
                x = position_data.attrib['x']
                y = position_data.attrib['y']
                z = position_data.attrib['z']
                for store in stores:
                    items = store.find("PlayerItems")

                    try:
                        items = items.findall("MyObjectBuilder_StoreItem")  # Get all the items
                        owner = store.find("Owner").text  # Get the ID

                        # Lookup the ID and change it to a name
                        owner = ids['Name'].loc[ids['ID'] == owner].values[0]

                    except:
                        items = []


                    for item in items:
                        print(item)
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
                            "Price per unit": price_per_unit,
                            'GPS String': 'GPS:' + grid_name + ':' + x + ':' + y + ':' + z + ':'
                        }
                        )
    return rows

'''End function definitions'''


#password = input("type password for db and press return or enter.")

'''Loop forever'''

while True:
    ls = os.listdir(".") #Get all the folders in the current directory
    all_stores_data = [] #initialize list
    engine = create_engine(name_or_url='mysql+pymysql://remote:Namaris@128.120.151.58:27000/economy',connect_args={'ssl':{"fake_flag_to_enable_tls":True}})#128.120.151.58 is hobo's test database
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine) #Metadata is the information on the schema. Kindof. IDK I'm not a developer

    for item in ls: #For all the folders
        '''OK this part got a little weird, hold on to your butts'''
        if os.path.isdir(os.path.join(os.path.abspath("."), item)) and 'Instance' in item: #If the folder name has the word Instance in it
            path = os.path.join(os.path.abspath("."), item) #Get the absolute path

            path2 = path+"/Instance/Saves" # The saves will always be under /Instance/Saves, so just add that string to the path
            sub_dir = os.listdir(path2) # list all sub dirs in that path

            sub_dir_paths = [] # make an empty list
            for dir in sub_dir: # loop through the subdirectories
                if os.path.isdir(os.path.join(os.path.abspath(path2), dir)) and "Expanse" in dir: #if it is a folder (as opposed to a file, AND it has the word "Expanse" in it

                    sub_dir_paths.append(os.path.join(os.path.abspath(path2), dir)) #Get all the save files in that sever folder

            #I don't think this line is needed, but if it's not broken, don't fix it.
            latest_subdir = max(sub_dir_paths, key=os.path.getctime) # get the newest save folder

            path3 = latest_subdir+"/Backup/" #Head on into the backup folder
            sub_dir = os.listdir(path3)  # Get the sub directories in the backup folder

            sub_dir_paths = [] #empty list again
            for dir in sub_dir:
                if os.path.isdir(os.path.join(os.path.abspath(path3), dir)):
                    sub_dir_paths.append(os.path.join(os.path.abspath(path3), dir))  # Get all the save files in that sever folder

            latest_subdir = max(sub_dir_paths, key=os.path.getctime)  # get the newest save file


            print("Processing save files in {}".format(latest_subdir))

            Server = serverName(latest_subdir) #Server name
            serverSQL(Server, conn, metadata) #Append the server name to the database if it doesn't exist
            ids = getPlayers(latest_subdir) #Get all the players so we can change IDs into names
            all_stores_data.append(getStore(latest_subdir,ids)) #Get all the stores and items in that sever

    # Flatten the list
    flat_list = [item for sublist in all_stores_data for item in sublist]

    if len(flat_list) > 0:
        # Make a dataframe
        df = pd.DataFrame(flat_list)

        dtypes = {
            "Server": types.TEXT,
            "Grid Name": types.TEXT,
            "X": types.FLOAT,
            "Y": types.FLOAT,
            "Z": types.FLOAT,
            "Owner": types.TEXT,
            "Item": types.TEXT,
            "Offer or Order": types.TEXT,
            "Qty": types.FLOAT,
            "Price per unit": types.FLOAT,
            'GPS String': types.TEXT
        }

        # df.to_csv("test.csv")
        df.to_sql('stores', con=engine, if_exists='replace', dtype=dtypes, chunksize=1000, method='multi') #Upload everything to the database

    conn.close()
    engine.dispose()
    print("Waiting 15 minutes....")
    time.sleep(900)
