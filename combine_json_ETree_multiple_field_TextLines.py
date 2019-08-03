import lxml.etree as etree
import json
import sys
from os import listdir
import re
from xml.dom.minidom import parseString
import ast
import binascii
from pymongo import MongoClient
import xml.etree.ElementTree as ET
import ast
import pyxdameraulevenshtein as lev
import dicttoxml




import cv2
import numpy as np
import json
import math
from copy import deepcopy
from ocr_pattern_hypothesis.utils import frame_utils
from ocr_pattern_hypothesis.frames.basic_frames import Word
from ocr_pattern_hypothesis.frames.structure.engine import StructureEngine
from ocr_pattern_hypothesis.frames.structure.text import  TextLine
import time
import os

mongo_ip="192.168.60.68"
client_name="ami-test"

"""
Created by @amandubey on 22/01/19
"""


######################################################################################################################

class k():
    def __init__(self):
        self.key=0
    def get_key(self):
        self.key=self.key+1
        return self.key

delta=k()

all_fields={}
relationship_pairs={}


# This function unrolls the relations in reverse order and puts them in relationship_pairs
#Using
def collect_children(page_id,parents):
    child_list=[]
    for each_child in parents:
        if each_child['field']:
            save_name=str(page_id+'_'+each_child['_id'])

        else:
            save_name=str(each_child['_id'])
        child_list.append(save_name)
        relationship_pairs[str(delta.get_key())]={'parent':save_name,'child':collect_children(page_id,each_child['children']),'is_link':not(each_child['field'])}

    return child_list

# Popping and structuring form relationship pairs


def clean_relationship_pairs(relationship_pairs):
    parents_list=[]
    keys_poped=[]
    links_list=[]
    for k,v in relationship_pairs.items():
        #Put every new parent in parents_list
        if v['parent'] not in parents_list:
            parents_list.append(v['parent'])
            #Put every link in links_list
            if v['is_link']:
                links_list.append(str(1+parents_list.index(v['parent'])))
        else:
            #If already present,it is a child. So extend child list of parent
            indx = parents_list.index(v['parent'])
            relationship_pairs[str(1 + indx)]['child'].extend(v['child'])
            keys_poped.append(k)
    for k in keys_poped:
        relationship_pairs.pop(k)

    relationship_pairs=clean_links(relationship_pairs,links_list)

    return relationship_pairs


def clean_links(relationship_pairs,links_list):
    for enum in links_list:
        for each_enum,val in relationship_pairs.items():
            if relationship_pairs[enum]['parent'] in val['child']:
                val['child'].extend(relationship_pairs[enum]['child'])
                val['child'].pop(val['child'].index(relationship_pairs[enum]['parent']))
    for k in links_list:
        relationship_pairs.pop(k)

    return relationship_pairs


def collect_all_fileds(page_id,fields):
    for each_data in fields:
        each_data['children']=[]
        all_fields[page_id+'_'+each_data['_id']]=each_data



def create_relation_fields(all_fields,relationship_pairs):
    poped_ids=[]
    field_list=[]
    for key_enum,tree_relation in relationship_pairs.items():
        for child_id in tree_relation['child']:
            all_fields[tree_relation['parent']]['children'].append(all_fields[child_id])
            poped_ids.append(child_id)

    for popid in poped_ids:
        all_fields.pop(popid)

    for k,v in all_fields.items():
        field_list.append(v)
    return field_list


def get_all_fields(mongo_ip,client_name,document_id):

    client = MongoClient(mongo_ip)
    db = client[client_name]
    relations = list(db.pageRelations.find({"document_id": document_id}))
    fields = list(db.fields.find({"documentId": document_id}))

    for enum,val in enumerate(relations):
        collect_children(val["page_id"],val["relations"])

    clean_relationship_pairs(relationship_pairs)
    for enum,val in enumerate(fields):
        collect_all_fileds(val["pageId"],val['fields'])
    for enum,val in enumerate(fields):
        collect_all_fileds(val["pageId"],val['tables'])

    field_list=create_relation_fields(all_fields,relationship_pairs)
    #print("Field List",field_list)

    return field_list


# Helper function to extract specific attributes according to field type for xml string
# for each parent and corresponding children and forming the xml structure

def get_field_xml(field,xml_data,print_flag=False):

    print(field)
    if not field['type']=='table' :
        label_and_value = ast.literal_eval(field['value'])

        if len(field['children']) > 0:
            for child in field['children']:
                b = ET.SubElement(xml_data, 'field')
                get_field_xml(child,b,print_flag=False)

        if field['type'] == "Key-value pair":
            value_word = label_and_value[1]['key']

        elif field['type'] == "group_frame" :
            if (len(label_and_value)==2) :
                value_word = label_and_value[1]['key']
            else:
                value_word = label_and_value[0]['key']

        else:
            value_word = label_and_value[0]['key']

        xml_data.set('value',value_word)
        xml_data.set('tag',field['tag'])

    return xml_data


def format_fields_for_xml(all_Fields):
    all_field_xml_data='<all_fields>'
    print("AllFields",all_Fields)
    for field in all_Fields:
        # Create proper xml string with parents and children in order for all relations present in all_fields
        root = ET.Element('field')
        al = ET.tostring(get_field_xml(field, root, print_flag=True))
        all_field_xml_data += al.decode("utf-8")
        # all_field_xml_data+=get_field_xml(field)

    all_field_xml_data+='</all_fields>'

    return all_field_xml_data


def get_page_coordinates_from_coordinates_data(coordinates_data_dict):
    """
    converts x,y,width,height to top,left,bottom,right
    :param coordinates_data_dict: dictionary having x,y,width,height values
    :return: tuple in top,left,bottom,right format
    """
    return (coordinates_data_dict['y'],coordinates_data_dict['x'],(coordinates_data_dict['y']
                                                                                    +coordinates_data_dict['height']),(coordinates_data_dict['x']+coordinates_data_dict['width']))


def get_coordinates_data_from_page_coordinates(page_coordinates):
    """
    converts top,left,bottom,right to x,y,width,height
    :param page_coordinates: list in top,left,bottom,right format
    :return: dictionary as x,y,width,height
    """
    print("page_coordinates",page_coordinates)
    return {'x':page_coordinates[1],'y':page_coordinates[0],'width':(page_coordinates[3]-page_coordinates[1]),
            'height':(page_coordinates[2]-page_coordinates[0])}


def is_coordinates_overlapping(rect1, rect2):
    """
    created by @amandubey on 13/11/18
    returns if two sets of coordinates are overlapping or not
    :param rect1: first set of coordinates
    :param rect2: second set of coordinates
    :return: True when two sets of coordinates are overlapping otherwise false
    """
    left = rect2[2] < rect1[0]
    right = rect1[2] < rect2[0]
    bottom = rect2[3] < rect1[1]
    top = rect1[3] < rect2[1]
    if top or left or bottom or right:
        return False
    else:               # rectangles intersect
        return True


def get_textlines(evidence,image):
    """
    Gives all textlines
    Any change to reflect in all the data present should be here
    """
    s_engine = StructureEngine((
        TextLine.generate,
    ))
    word_patches_dict = {}
    structures = []
    for each_evidence in evidence['words']:
        # print("txtline keys :",each_evidence.keys())
        # print(each_evidence['coordinate']['y'])
        # print(each_evidence['coordinate']['x'])
        # print(each_evidence['coordinate']['width'])
        # print(each_evidence['coordinate']['height'])
        label_word = str(each_evidence['label'])

        coordinates = (each_evidence['coordinate']['y'],each_evidence['coordinate']['x'],(each_evidence['coordinate']['height'] + each_evidence['coordinate']['y']),(each_evidence['coordinate']['width']+each_evidence['coordinate']['x']))

        xx=re.findall(r'[a-zA-Z0-9-+=-_]+', label_word)

        if len(xx)<1:
            label_word=" "

        label_word=label_word.replace('"',"'")
        word_patches_dict[coordinates] = label_word

    try:
        structures = s_engine.run(image, word_args=(word_patches_dict,))
    except IndexError:
        structures = []
    structures=structures.filter(TextLine)
    print('no. of textlines',len(structures))
    return structures


def textline_intersection(rect1,rect2):
    """
    Get new coordinates if two blocks are overlapping
    :return: merged coordinates
    """
    t = min((rect1[0], rect2[0]))
    l = min((rect1[1], rect2[1]))
    b = max((rect1[2], rect2[2]))
    r = max((rect1[3], rect2[3]))
    return [t, l,b, r]


def fetch_textline_xml_data(value_word,tag,text_root,label_word="",has_label=False):
    """

    :param value_word: value
    :param tag: tag
    :param label_word: label
    :param has_label: kv pair
    :param ends_here: child
    :param is_end_of_data: end of total text node block
    :return: formatted text node string
    """

    if has_label:
        text_root.set('key', str(label_word))

    text_root.set('value', str(value_word))
    text_root.set('tag', str(tag))

    return ET


def fetch_textline_xml_data_with_children(value_word,tag,text_root) :

    text_root.set('value',value_word)
    text_root.set('tag',tag)

    return text_root


def get_attributes(field, label_and_value):
    value_word, label_word = '', ''

    has_label = False
    if field['type'] == "Key-value pair":
        has_label = True
        label_word = label_and_value[0]['key']
        value_word = label_and_value[1]['key']


    #######################################################
    # Added by Ayan
    elif field['type'] == "group_frame":
        if (len(label_and_value) == 2):
            has_label = True
            label_word = label_and_value[0]['key']
            value_word = label_and_value[1]['key']
        else:
            value_word = label_and_value[0]['key']
            label_word = ""

    #######################################################

    elif field['type'] == "":  # Standalone
        value_word = label_and_value[0]['key']
        label_word = ""

    return [has_label, value_word, label_word]


def create_textline_level_txtNodes(page_structure,tables_list,fields_list):
    finalised_page_structures="<all_txtNodes>"
    textline_coordinates=[0,0,0,0]
    for textline in page_structure:
        is_finalised_page_structures=True
        textline_coordinates[0] = (textline.coordinates[0][1])
        textline_coordinates[1] = (textline.coordinates[0][0])
        textline_coordinates[2] = (textline.coordinates[1][1])
        textline_coordinates[3] = (textline.coordinates[1][0])
        list_o_fields = []
        popped = []

        for each_table in tables_list:
            if is_coordinates_overlapping(textline_coordinates,get_page_coordinates_from_coordinates_data(each_table['coordinates'])):
                is_finalised_page_structures=False

        if is_finalised_page_structures:
            if str(textline)=="(2) Total Gas Cost":
                print('"(2) Total Gas Cost"')

            text_root = ET.Element('txtNode')
            textline_print_flag=False

            for k,e_f in enumerate(fields_list) :

                if is_coordinates_overlapping(textline_coordinates,get_page_coordinates_from_coordinates_data(e_f['coordinate'])):
                    has_label=False
                    list_o_fields.append(e_f)
                    popped.append(k)

            if len(list_o_fields) == 1:
                # If the particular field(each_field) exactly matches with the text line
                label_and_value = ast.literal_eval(list_o_fields[0]['value'])
                lis = get_attributes(list_o_fields[0], label_and_value)

                has_label = lis[0]
                value_word = lis[1]
                label_word = lis[2]

                if not lev.normalized_damerau_levenshtein_distance(value_word, str(textline)) and (len(list_o_fields) == 1):
                    fetch_textline_xml_data(value_word=value_word, tag=list_o_fields[0]['tag'],
                                                  label_word=label_word, has_label=has_label, text_root=text_root)
                    # fps = fps.tostring(text_root).decode("utf-8")
                    # finalised_page_structures += fps
                    textline_print_flag = True

                else :
                    if not textline_print_flag:
                        # finalised_page_structures+=fetch_textline_xml_data_with_children(value_word=str(textline),tag="",text_root=text_root)
                        text_root = fetch_textline_xml_data_with_children(value_word=str(textline), tag="",
                                                                          text_root=text_root)
                        textline_print_flag = True

                    b = ET.SubElement(text_root, 'txtNode')
                    fetch_textline_xml_data(value_word=value_word, tag=list_o_fields[0]['tag'],
                                            label_word=label_word, has_label=has_label, text_root=b)

                fps = ET.tostring(text_root).decode("utf-8")
                finalised_page_structures+= fps

            if len(list_o_fields) > 1 :

                text_root = fetch_textline_xml_data_with_children(value_word=str(textline), tag="", text_root=text_root)

                for each_field in list_o_fields :
                    label_and_value = ast.literal_eval(each_field['value'])
                    lis = get_attributes(list_o_fields[0], label_and_value)

                    has_label = lis[0]
                    value_word = lis[1]
                    label_word = lis[2]

                    b = ET.SubElement(text_root,'txtNode')
                    fetch_textline_xml_data(value_word=value_word,tag=each_field['tag'],
                                                             label_word=label_word,has_label=has_label, text_root=b)

                fps = ET.tostring(text_root).decode("utf-8")
                finalised_page_structures+= fps

                for kp in popped:
                    fields_list.pop(kp)


            #Logic if no field present in textline
            if not textline_print_flag:
                fps = fetch_textline_xml_data(value_word=str(textline), tag="", text_root=text_root)
                fps = fps.tostring(text_root).decode("utf-8")
                finalised_page_structures += fps

    finalised_page_structures+='</all_txtNodes>'

    return finalised_page_structures


def check_tag_key(data_dict):
    if 'tag' not in data_dict.keys():
        data_dict['tag']=['']
    return data_dict


def format_xml_tag_for_table(tags):
    return ','.join(tags)


def fetch_table_row_xml_data(inside_table_data,headers, inside_table_xml, type='row'):
    # inside_table_xml=''                  # Pass this to function
    # inside_table_xml = ET.Element('tableRow')
    for enum,each_itter_data in enumerate(inside_table_data):
        each_itter_data=check_tag_key(each_itter_data)

        if type=='row':
            # inside_table_xml+='<tableRow tags="'+format_xml_tag_for_table(each_itter_data['tag'])+'">'
            b = ET.SubElement(inside_table_xml, 'tableRow')
            b.set('tags', format_xml_tag_for_table(each_itter_data['tag']))
            fetch_table_row_xml_data(each_itter_data['cells'], headers, inside_table_xml,
                                                         type='column')

        elif type=='column':
            b = ET.SubElement(inside_table_xml, 'tableCell')
            # inside_table_xml += '<tableCell value="' + each_itter_data['value'] + '" tags="' + format_xml_tag_for_table(
            #     each_itter_data['tag']) + '" column="' + headers[enum] + '"/>'

            b.set('value', each_itter_data['value'])
            b.set('tags', format_xml_tag_for_table(each_itter_data['tag']))
            b.set('column', headers[enum])

    return inside_table_xml


def check_table_for_headers(first_row):
    headers=[]
    hasHeader=False
    if first_row['isHeader']:
        for cell in first_row['cells']:
            headers.append(cell['value'])
        hasHeader=True
    else:
        for _ in first_row['cells']:    # cell
            headers.append('')
    return hasHeader,headers


def create_txtNodes_for_table(all_tables_list):

    if len(all_tables_list)<1:
        return "<all_tables />"

    else :
        table_data_xml='<all_tables>'
        for each_table in all_tables_list:
            # print('#'*30)
            # table_data_xml+='<table label="'+str(each_table['label'])+'">'
            # table_data_xml = ''
            inside_table_xml = ET.Element('table')
            inside_table_xml.set('label', str(each_table['label']))

            hasHeaders,headers=check_table_for_headers(each_table['tableRows'][0])

            # inside_table_xml = ET.Element()
            if hasHeaders:
                table_data_xml+= ET.tostring(fetch_table_row_xml_data(each_table['tableRows'][1:],headers, inside_table_xml=inside_table_xml)).decode('utf-8')
            else:
                table_data_xml+= ET.tostring(fetch_table_row_xml_data(each_table['tableRows'],headers, inside_table_xml=inside_table_xml)).decode('utf-8')

            # table_data_xml+='</table>'

        table_data_xml+='</all_tables>'
        # print("all tables")
        # print(table_data_xml)

    return table_data_xml


def fetch_page_level_info(mongo_ip,client_name,document_id,image_path="/home/amandubey/Documents/"):
    client = MongoClient(mongo_ip)
    db = client[client_name]
    # all_document_related_pages_info = list(db.pages.find({"documentId": document_id}))
    all_document_related_pages_info = list(db.pages.find({"documentId": document_id}).sort('pageNumber'))

    all_document_related_fields = list(db.fields.find({"documentId": document_id}).sort('pageNumber'))
    all_pages_xml="<Page_Data>"

    xml_list=[]
    for enum,each_page in enumerate(all_document_related_pages_info):
        each_page_xml='<Page page_number="'+str(int(each_page['path'][-5])+1)+'">'

        for each_field in all_document_related_fields:
            if each_field['path']==each_page['path']:
                page_related_fields=each_field
                break
        # page_image=cv2.imread(image_path+document_id+'/'+each_page['path'])
        page_image=cv2.imread("/Users/ayanbask/Desktop/IDP/flower_image.jpg")
        page_structure=get_textlines(each_page,page_image)
        each_page_xml +=create_textline_level_txtNodes(page_structure,page_related_fields['tables'],page_related_fields['fields'])
        each_page_xml+=create_txtNodes_for_table(page_related_fields['tables'])
        each_page_xml+='</Page>'
        all_pages_xml+=each_page_xml
    # for i in range(len(xml_list)-1,-1,-1):
    #     all_pages_xml+=xml_list[i]
    all_pages_xml+='</Page_Data>'
    # print("all page XML IS")
    # print(all_pages_xml)
    return all_pages_xml


def combine_json_parse_xml( document_id):
    data_list = []
    data = {}
    # document_id=uploadpath.split('/')[-1]

    # final_data_xml -> Final structured xml string
    final_data_xml='<root>'
    # Fetching all fields by document
    data['all_Fields']=get_all_fields(mongo_ip,client_name,document_id)
    # Cleaning the fields by extracting only required info
    final_data_xml+=format_fields_for_xml(data['all_Fields'])
    final_data_xml+=fetch_page_level_info(mongo_ip,client_name,document_id)
    final_data_xml+='</root>'

    print('final_data_xml')
    print(final_data_xml)
    f=open("xml_data.xml",'w')
    f.write(final_data_xml)
    f.close()


if __name__ == '__main__':
    # a={'as':['wq','ew','2332','asad','lsakd']}
    # print(a['as'][2:])
    # exit()
    # uploadpath = sys.argv[1]
    # uploadpath = "/home/amandubey/Downloads/5c530a120ed0a632bcaf797c/"
    # doc_id="5d43de726f23737fdfd63cc3"
    doc_id="5d427bc96f237378706564e4"
    combine_json_parse_xml(doc_id)
    print("success")

# paragraphs_text = detect_paragraph(img, evidence, 1.5,False,2)
