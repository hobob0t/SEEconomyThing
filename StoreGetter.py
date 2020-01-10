import xml.etree.cElementTree as etree
import xml.etree.ElementTree as ET
import pandas as pd

'''Get server name from save file'''
tree = ET.parse('save/Sandbox.sbc')
root = tree.getroot()
Server = (root.find("SessionName").text)

players = root.iter("MyObjectBuilder_Identity")

'''Find all the IDs and players so we can change ID into player name later'''
ids = []
for player in players:
    ids.append(
        {
            "Name":player.find("DisplayName").text,
            "ID":player.find("IdentityId").text

        }
    )

ids = pd.DataFrame(ids)

'''Now parse the other save file for store data'''
tree = ET.parse('save/SANDBOX_0_0_0_.sbs')
root = tree.getroot()

rows=[]

#Get all entities
for grid in root.iter('MyObjectBuilder_EntityBase'):
    #If the entity is a grid
    if grid.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_CubeGrid":

        grid_name = grid.find("DisplayName").text #The grid name
        blocks = grid.find("CubeBlocks") #The blocks in the grid
        position_data = grid.find("PositionAndOrientation").find("Position") #The position of the grid
        x = position_data.attrib['x']
        y = position_data.attrib['y']
        z = position_data.attrib['z']

        for cube in blocks:
            #If the block is a store
            if cube.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_StoreBlock":
                items = cube.find("PlayerItems").findall("MyObjectBuilder_StoreItem") #Get all the items
                owner = cube.find("Owner").text #Get the ID

                #Lookup the ID and change it to a name
                owner = ids['Name'].loc[ids['ID']==owner].values[0]

                for item in items:
                    item_name=item.find("Item").attrib["Subtype"] #The item name
                    item_type = item.find("StoreItemType").text #Offer or order
                    price_per_unit=item.find("PricePerUnit").text #Price per unit
                    quantity = item.find("Amount").text #Qty

                    #This is a neat trick for making dataframes. It's a list of dictionaries
                    rows.append({
                        "Server":Server,
                        "Grid Name":grid_name,
                        "X":x,
                        "Y":y,
                        "Z":z,
                        "Owner":owner,
                        "Item":item_name,
                        "Offer or Order":item_type,
                        "Qty":quantity,
                        "Price per unit":price_per_unit
                    }
                    )

#Make a dataframe
df = pd.DataFrame(rows)

#Save a dataframe
df.to_csv("test.csv")

