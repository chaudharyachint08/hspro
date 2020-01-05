import json
import xmltodict as x2j

# import json2xml  as j2x

import dicttoxml  as j2x
from xml.dom.minidom import parseString


def xml2json(xml_path,json_path,mode='string'):
    "Converts XML to JSON on Directory"
    '''
    This file can be further improved, as values are all string in JSON created
    A recursive formulation can be used to convert them to their data-types
    Also, "Item" key in results is redundant, while de don't need all this for now
    Since, only final costs are required they can be extracted by eval() on root value of costs
    '''
    if mode=='string':
        json_obj = x2j.parse(xml_path)
        return json_obj
    else:
        with open(xml_path) as in_file:
            xml = in_file.read()
            with open(json_path, 'w') as out_file:
                out_file.write( json.dumps( x2j.parse(xml), indent=2) )
        with open(json_path) as in_file:
            json_plan = json.load(in_file)
            res = json_plan["explain"]["Query"]["Plan"]
            out_json = {"QUERY PLAN":[{"Plan":res}]}
        with open(json_path,'w') as out_file:
            out_file.write(json.dumps(out_json, indent=2))

def json2xml(json_path,xml_path):
    "Converts JSON to XML on Directory"
    '''
    XML generated should not be tweaked and also is not reliable for FPC or Plan Enforcing purposes
    '''
    with open(json_path) as in_file:
        json_plan = json.load(in_file)
        xml = j2x.dicttoxml(json_plan)
        dom = parseString(xml)
        with open(xml_path,'w') as out_file:
            print(dom.toprettyxml(),file=out_file)