from collections import defaultdict
import lxml.etree as ET
import os
import xmltodict

def get_xsl_template_path(file_name):
    xsl_templates_path = os.path.join(os.path.dirname(__file__), 'xsl_templates')
    file_path = os.path.join(xsl_templates_path, file_name)
    return file_path

def parse_xml(input_xml):
    return ET.parse(input_xml)

def parse_dm_xml_to_dict(input_xml):
    from airflow.exceptions import AirflowException

    with open(input_xml, 'r') as xml_file:
         xml_doc = xmltodict.parse(xml_file.read())

    if 'document' not in xml_doc.keys():
        raise AirflowException("Unexpected XML Data, expected DailyMed Document")
    
    return xml_doc

def transform_xml_to_dict(input_xml, xslt):
    
    dom =  ET.parse(input_xml)
    xslt_doc = ET.parse(xslt)
    xslt_transformer = ET.XSLT(xslt_doc)
    new_xml = xslt_transformer(dom)
    root = ET.fromstring(ET.tostring(new_xml))
    
    result_dict = defaultdict(list)
    
    for elem in root.iter():
        if elem.tag not in result_dict:
            result_dict[elem.tag] = []
        if elem.text is not None: 
            result_dict[elem.tag].append(elem.text)
    
    return dict(result_dict)


def transform_xml(input_xml, xslt):
    # load xml input
    dom = ET.parse(input_xml)
    # load XSLT
    xslt_doc = ET.parse(xslt)
    xslt_transformer = ET.XSLT(xslt_doc)
    # apply XSLT on loaded dom
    new_xml = xslt_transformer(dom)
    return ET.tostring(new_xml, pretty_print=True).decode("utf-8")