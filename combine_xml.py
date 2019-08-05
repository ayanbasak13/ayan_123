from xml.etree import ElementTree as ET


def combine_xml(files):
    final_xml = None
    for filename in files:
        data = ET.parse(filename).getroot()
        if final_xml is None:
            final_xml = data
        else:
            final_xml.extend(data)
    if final_xml is not None:
        return ET.tostring(final_xml)


files = ["/Users/ayanbask/Desktop/data_0.xml", "/Users/ayanbask/Desktop/data_1.xml"]

s = combine_xml(files).decode("utf-8")

f = open("/Users/ayanbask/Desktop/combined_xml_data.xml", 'w')
f.write(s)
f.close()