import xml.etree.cElementTree as etree
import xml.etree.ElementTree as ET
import pandas as pd

tree = ET.parse('save/SANDBOX_0_0_0_.sbs')
root = tree.getroot()

rows=[]

for grid in root.iter('MyObjectBuilder_EntityBase'):
    if grid.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_CubeGrid":
        grid_name = grid.find("DisplayName").text
        blocks = grid.find("CubeBlocks")
        position_data = grid.find("PositionAndOrientation").find("Position")
        x=position_data.attrib['x']
        y=position_data.attrib['y']
        z=position_data.attrib['z']

        for cube in blocks:

            if cube.attrib['{http://www.w3.org/2001/XMLSchema-instance}type'] == "MyObjectBuilder_StoreBlock":
                items = cube.find("PlayerItems").findall("MyObjectBuilder_StoreItem")
                owner = cube.find("Owner").text
                for item in items:
                    item_name=item.find("Item").attrib["Subtype"]
                    item_type = item.find("StoreItemType").text
                    price_per_unit=item.find("PricePerUnit").text
                    quantity = item.find("Amount").text

                    rows.append({
                        "Grid Name":grid_name,
                        "X":x,
                        "Y":y,
                        "Z":z,
                        "Owner":owner,
                        "Item":item_name,
                        "Offer or Order":item_type,
                        "Qty":quantity
                    }
                    )
df = pd.DataFrame(rows)

# for cube in root.iter('MyObjectBuilder_CubeBlock'):
#     if cube.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']=="MyObjectBuilder_StoreBlock":
#         items=cube.find("PlayerItems").findall("MyObjectBuilder_StoreItem")
#         for item in items:
#             print(item.find("Item").attrib["Subtype"])
#             print(item.find("StoreItemType").text)
#             print(item.find("PricePerUnit").text)
#             print(item.find("Amount").text)
#
