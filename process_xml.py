from urllib.request import urlopen
import xml.etree.ElementTree as ET
import pandas as pd

def main(margins = False,decline = False, size = False):
    url = urlopen('LINK TO XML FEED')
    ns = {'g': 'http://base.google.com/ns/1.0'} # this is if you have a namespace
    xmldoc = ET.parse(url)
    
    data = []
    for item in xmldoc.findall('channel/item',ns):
        if margins:
            marg = item.find('g:custom_label_X',ns).text
            gid = item.find('g:item_group_id',ns).text
            brand = item.find('g:brand',ns).text
            data += [[marg, gid, brand]]
        elif decline:
            gid = item.find('g:item_group_id',ns).text
            img = item.find('g:image_link',ns).text
            brand = item.find('g:brand',ns).text
            data += [[gid, img, brand]]
        elif size:
            gSize = item.find('g:size',ns).text
            gItemid = item.find('g:id',ns).text
            data += [[gSize, gItemid]]
        else:
            gid = item.find('g:item_group_id',ns).text
            gItemid = item.find('g:id',ns).text
            gtin = item.find('g:gtin',ns).text
            brand = item.find('g:brand',ns).text
            img = item.find('g:image_link',ns).text
            data += [[gid, gItemid, gtin, img, brand]]
        
    if margins:
        df = pd.DataFrame(data, columns = ['Margin','GID', 'Brand'])
    elif decline:
        df = pd.DataFrame(data, columns = ['GID', 'IMG', 'Brand'])
    elif size:
        df = pd.DataFrame(data, columns = ['Size', 'ID'])
    else:
        df = pd.DataFrame(data, columns = ['GID','ID', 'GTIN','IMG', 'BRAND'])
        
    return df
if __name__ == "__main__":
    main()