import os
import os.path
import shutil
import pathlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from bdbag import bdbag_api
from bagit import BagValidationError
from bdbag.bdbagit import BaggingInterruptedError
from pyrsistent import thaw
from zipfile import ZipFile
from openpyxl import Workbook
from os.path import basename

#------------------------------------------------------------------------------------------------------------------------------------------------------
# WORKFLOW FOR PREPARING ASSETS FOR PRESERVICA INGEST
# Assumes that access assets are coming from Islandora
# Assumes that preservation assets are coming from Digital Scholarship
# Assumes that metadata is coming from Islandora
#------------------------------------------------------------------------------------------------------------------------------------------------------
#   ----manual process---------------------------------------------------------------------------------------------------------------------------------
# X create_container() - Reanme 'ds_files' directory into container directory which will be dumped into WinSCP for OPEX incremental ingest
# X Manual Process - COPY preservations masters directory into root of project folder
# X folder_ds_files() - Transform single directory of preservation images into separate subdirectories full of images per asset
# X create_bags_dir() -  Create bags directory to stage exported bags for processing
# X Manual Process - COPY zipped bags over into created bags directory
# X extract_bags() - Extract/unzip the bags in the bags directory
# X validate_bags() - Validate the unzipped bags to ensure no errors in transfer
# X create_id_ss() - Create a spreadsheet with the mapping between preservation file names, access file names, and bag ids
# X Manual Process - Rectify the mismatches presented in the pres_acc_bag_ids spreadsheet
#   ----can be run in sequnce--------------------------------------------------------------------------------------------------------------------------
# X representation_preservation() - Create 'Representation_Preservation' subdirectories in each asset folder, then move preservation assets into them
# X process_bags() - Reverts the bags into simple directories and removes unnecessary files in 'data' subdirectory
# X representation_access() - Create 'Representation_Access' subdirectories in each asset folder
# X access_id_path() - Generate file containing MODS identifier and relative paths
# X merge_access_preservation() - Loop through dirs in container, move access copies and metadata into relevant folders
# X cleanup_bags() - Delete the bags_dir folder and the access_ids.txt file once merge is complete
# X pax_metadata() - Write the OPEX metadata for the individua assets contained in the PAX
# X stage_pax_content() - moves the Representation_Access and Representation_Preservation folders into a staging directory to enable zipping the PAX
# X create_pax() - Make a PAX zip archive out of the Representation_Access and Representation_Access
# X cleanup_directories() - Delete the xml files used to create the OPEX metadata and the directories used to create the PAX zip archive
# X ao_opex_metadata() - Create the OPEX metadata for the archival object folder that syncs with ArchivesSpace, and rename subdirectories
# X write_opex_container_md() - Write the OPEX metadata for the entire container structure
#------------------------------------------------------------------------------------------------------------------------------------------------------
# Project Log File variables by index
# 0 - date_time
# 1 - container
# 2 - bags_dir
#------------------------------------------------------------------------------------------------------------------------------------------------------

proj_path = 'M:/IDT/DAM/McGraw_Preservica_Ingest'
#copy folder name provided by DS into *ds_files* variable
#ds_files is child of preservica_ingest, top level folder
ds_files = 'preservation_masters'

def create_container():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'a')
    now = datetime.now()
    date_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    project_log_hand.write(date_time + '\n')
    container = 'container_' + date_time
    os.rename(proj_path + '/' + ds_files, proj_path + '/' + container)
    project_log_hand.write(container + '\n')
    print('Container directory: {}'.format(container))
    project_log_hand.close()

def folder_ds_files():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    date_time = vars[0]
    date_time = date_time.strip()
    container = vars[1]
    container = container.strip()
    folder_name = ''
    folder_count = 0
    file_count = 0
    for file in os.listdir(path = proj_path + '/' + container):
        file_root = file.split('-')
        file_root = file_root[0]
        if  file_root == folder_name:
            shutil.move(proj_path + '/' + container + '/' + file, proj_path + '/' + container + '/' + folder_name + '/' + file)
            file_count += 1
        else:
            folder_name = file_root
            os.mkdir(proj_path + '/' + container + '/' + folder_name)
            folder_count += 1
            shutil.move(proj_path + '/' + container + '/' + file, proj_path + '/' + container + '/' + folder_name + '/' + file)
            file_count += 1
    for folder in os.listdir(path = proj_path + '/' + container):
        count = 0
        for file in os.listdir(proj_path + '/' + container + '/' + folder):
            count += 1
        if count > 99:
            os.rename(proj_path + '/' + container + '/' + folder, proj_path + '/' + container + '/' + folder + "-001-" + str(count))
        elif count > 9:
            os.rename(proj_path + '/' + container + '/' + folder, proj_path + '/' + container + '/' + folder + "-001-0" + str(count))
        else:
            os.rename(proj_path + '/' + container + '/' + folder, proj_path + '/' + container + '/' + folder + "-001-00" + str(count))
    print('Created and renamed {} subdirectories and moved {} files into them'.format(folder_count, file_count))
    project_log_hand.close()

def create_bags_dir():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    date_time = vars[0]
    date_time = date_time.strip()
    container = vars[1]
    container = container.strip()
    bags_dir = 'bags_' + date_time
    os.mkdir(proj_path + '/' + container + '/' + bags_dir)
    project_log_hand.close()
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'a')
    project_log_hand.write(bags_dir + '\n')
    print('Created bags directory: {}'.format(bags_dir))
    project_log_hand.close()

def extract_bags():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    num_bags = 0
    for file in os.listdir(path = proj_path + '/' + container + '/' + bags_dir):
        bdbag_api.extract_bag(proj_path + '/' + container + '/' + bags_dir + '/' + file, output_path = proj_path + '/' + container + '/' + bags_dir, temp=False)
        print('extracting bag: {}'.format(file))
        num_bags += 1
    for bag in os.listdir(path = proj_path + '/' + container + '/' + bags_dir):
        if bag.endswith('.zip'):
            print('removing zipped bag: {}'.format(bag))
            os.remove(proj_path + '/' + container + '/' + bags_dir + '/' + bag)
    print('Extracted {} bags'.format(str(num_bags)))
    project_log_hand.close()

def validate_bags():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    error_log_handle = open(proj_path + '/' + 'validation_error_log.txt', 'a')
    num_bags = 0
    for directory in os.listdir(path = proj_path + '/' + container + '/' + bags_dir):
        print('attempting to validate {}'.format(directory))
        num_bags += 1
        try:
            bdbag_api.validate_bag(proj_path + '/' + container + '/' + bags_dir + '/' + directory, fast = False)
        except BagValidationError:
            error_log_handle.write('Bag Validation Error | Directory: ' + directory + '\n')
        except BaggingInterruptedError:
            error_log_handle.write('Bagging Interruped Error | Directory: ' + directory + '\n')
        except RuntimeError:
            error_log_handle.write('Runtime Error | Directory: ' + directory + '\n')
    print('Validated {} bags'.format(str(num_bags)))
    error_log_handle.close()
    project_log_hand.close()

def create_id_ss():
    wb = Workbook()
    ws = wb.active
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    project_log_hand.close()
    ws['A1'] = 'pres_file_name'
    ws['B1'] = 'acc_file_name'
    ws['C1'] = 'bag_id'
    pres_file_list = []
    for folder in os.listdir(path = proj_path + '/' + container):
        if folder.startswith('bags_'):
            continue
        else:
            pres_file_list.append(folder)
    bag_dict = dict()
    for bag in os.listdir(path =  proj_path + '/' + container + '/' + bags_dir):
        tree = ET.parse(proj_path + '/' + container + '/' + bags_dir + '/' + bag + '/data/MODS.xml')
        identifier = tree.find('{http://www.loc.gov/mods/v3}identifier').text
        bag_dict[identifier] = bag
    for item in pres_file_list:
        if item in bag_dict.keys():
            ws.append([item, item, bag_dict[item]])
        else:
            ws.append([item, '', ''])
    for item in bag_dict.keys():
        if item not in pres_file_list:
            ws.append(['', item, bag_dict[item]])
    wb.save('pres_acc_bag_ids.xlsx')
    print('Created pres_acc_bag_ids.xlsx')

def representation_preservation():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    folder_count = 0
    file_count = 0
    rep_pres = 'Representation_Preservation'
    for directory in os.listdir(path = proj_path + '/' + container):
        if directory.startswith('bags_'):
            continue
        path = proj_path + '/' + container + '/' + directory + '/' + rep_pres
        os.mkdir(path)
        folder_count += 1
        for file in os.listdir(path = proj_path + '/' + container + '/' + directory):
            if file == rep_pres:
                continue
            else:
                file_name = file.split('.')
                file_name = file_name[0]
                os.mkdir(path + '/' + file_name)
                shutil.move(proj_path + '/' + container + '/' + directory + '/' + file, path + '/' + file_name + '/' + file)
            file_count += 1
    print('Created {} Representation_Preservation directories | Moved {} files into created directories'.format(folder_count, file_count))
    project_log_hand.close()

def process_bags():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    num_bags = 0
    error_log_str = ''
    error_log_handle = open(proj_path + '/' + 'validation_error_log.txt', 'r')
    error_log = error_log_handle.read()
    for line in error_log:
        error_log_str = error_log_str + line
    for directory in os.listdir(path = proj_path + '/' + container + '/' + bags_dir):
        #skips any directories that raised errors during validation
        if error_log_str.find(directory) != -1 :
            continue
        else:
            print('attempting to revert bag: {}'.format(directory))
            obj_file_name = ''
            #converts the bags back into normal directories, removing bagit and manifest files
            bdbag_api.revert_bag(proj_path + '/' + container + '/' + bags_dir + '/' + directory)
            #removes unnecessary files generated by Islandora
            unneccesary_files = ['foo.xml', 'foxml.xml', 'JP2.jp2', 'JPG.jpg', 'POLICY.xml', 'PREVIEW.jpg', 'RELS-EXT.rdf', 'RELS-INT.rdf', 'TN.jpg', 'HOCR.html', 'OCR.txt', 'MP4.mp4', 'PROXY_MP3.mp3']
            for file in os.listdir(path = proj_path + '/' + container + '/' + bags_dir + '/' + directory):
                if file in unneccesary_files:
                    os.remove(proj_path + '/' + container + '/' + bags_dir + '/' + directory + '/' + file)
                if re.search('^OBJ', file):
                    obj_file_name = file
                    extension = obj_file_name.split('.')
                    extension = extension[1]
                    extension = extension.strip()
                elif re.search('^PDF', file):
                    obj_file_name = file
                    extension = obj_file_name.split('.')
                    extension = extension[1]
                    extension = extension.strip()
            #use xml.etree to identify filename from MODS.xml
            tree = ET.parse(proj_path + '/' + container + '/' + bags_dir + '/' + directory + '/MODS.xml')
            identifier = tree.find('{http://www.loc.gov/mods/v3}identifier').text
            #rename the OBJ file to original filename pulled from MODS.xml
            os.rename(proj_path + '/' + container + '/' + bags_dir + '/' + directory + '/' + obj_file_name, proj_path + '/' + container + '/' + bags_dir + '/' + directory + '/' + identifier + '.' + extension)
        num_bags += 1
    error_log_handle.close()
    print('Processed {} bags'.format(str(num_bags)))
    project_log_hand.close()

def representation_access():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    folder_count = 0
    rep_acc = 'Representation_Access'
    for directory in os.listdir(path = proj_path + '/' + container):
        if directory.startswith('bags_'):
            print('bags_ folder found - skipped')
        else:
            path = proj_path + '/' + container + '/' + directory + '/' + rep_acc
            os.mkdir(path)
            print('created {}'.format(path))
        folder_count += 1
    print('Created {} Representation_Access directories'.format(folder_count))
    project_log_hand.close()

def access_id_path():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    access_id_hand = open(proj_path + '/' + 'access_ids.txt', 'a')
    access_count = 0
    for directory in os.listdir(path = proj_path + '/' + container + '/' + bags_dir):
        tree = ET.parse(proj_path + '/' + container + '/' + bags_dir + '/' + directory + '/MODS.xml')
        identifier = tree.find('{http://www.loc.gov/mods/v3}identifier').text
        access_id_hand.write(identifier + '|')
        path = proj_path + '/' + container + '/' + bags_dir + '/' + directory
        access_id_hand.write(path + '\n')
        access_count += 1
        print('logged {} and {}'.format(identifier, path))
    print('Logged {} paths and identifiers in access_ids.txt'.format(access_count))
    project_log_hand.close()
    access_id_hand.close()

# NOTE this process created 'Thumbs' directories in Representation_Preservation subdirs for an unknown reason
def merge_access_preservation():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    access_id_hand = open(proj_path + '/' + 'access_ids.txt', 'r')
    access_id_list = access_id_hand.readlines()
    rep_acc = 'Representation_Access'
    file_count = 0
    for directory in os.listdir(path = proj_path + '/' + container):
        if directory.startswith('bags_'):
            continue
        else:
            for line in access_id_list:
                print('merging {} and {}'.format(directory,line))
                access_info = line.split('|')
                identifier = access_info[0]
                identifier = identifier.strip()
                path = access_info[1]
                path = path.strip()
                if identifier == directory:
                    for file in os.listdir(path = path):
                        if file.endswith('.xml'):
                            shutil.move(path + '/' + file, proj_path + '/' + container + '/' + directory + '/' + file)
                            file_count += 1
                        else:
                            file_name = file.split('.')
                            file_name = file_name[0]
                            os.mkdir(proj_path + '/' + container + '/' + directory + '/' + rep_acc + '/' +  file_name)
                            shutil.move(path + '/' + file, proj_path + '/' + container + '/' + directory + '/' + rep_acc + '/' + file_name + '/' + file)
                            file_count += 1
    print('Moved {} access and metadata files'.format(file_count))
    project_log_hand.close()
    access_id_hand.close()

def cleanup_bags():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    shutil.rmtree(proj_path + '/' + container + '/' + bags_dir)
    os.remove('access_ids.txt')
    print('Deleted "{}" directory and access_ids.txt'.format(bags_dir))
    project_log_hand.close()

def pax_metadata():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    dir_count = 0
    for directory in os.listdir(path = proj_path + '/' + container):
        if directory == container + '.opex':
            continue
        else:
            opex1 = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Properties><opex:Title>'
            tree = ET.parse(proj_path + '/' + container + '/' + directory + '/DC.xml')
            root = tree.getroot()
            opex2 = tree.find('{http://purl.org/dc/elements/1.1/}title').text
            opex3 = '</opex:Title><opex:Identifiers>'
            id_list = []
            opex4 = ''
            for id in root.findall('{http://purl.org/dc/elements/1.1/}identifier'):
                id_list.append(id.text)
            for item in id_list:
                if item.startswith('ur'):
                    opex4 += '<opex:Identifier type="code">' + item + '</opex:Identifier>'
                else:
                    other_identifiers = item.split(':')
                    label = other_identifiers[0]
                    label = label.strip()
                    value = other_identifiers[1]
                    value = value.strip()
                    opex4 += '<opex:Identifier type="' + label + '">' + value + '</opex:Identifier>'
            opex5 = '</opex:Identifiers></opex:Properties><opex:DescriptiveMetadata><LegacyXIP xmlns="http://preservica.com/LegacyXIP"><AccessionRef>catalogue</AccessionRef></LegacyXIP>'
            opex6 = ''
            for file in os.listdir(path = proj_path + '/' + container + '/' + directory):
                if file.endswith('.xml'):
                    temp_file_hand = open(proj_path + '/' + container + '/' + directory + '/' + file, 'r')
                    lines = temp_file_hand.readlines()
                    for line in lines:
                        opex6 += line
                    temp_file_hand.close()
            opex7 = '</opex:DescriptiveMetadata></opex:OPEXMetadata>'
            filename = directory + '.pax.zip.opex'
            pax_md_hand = open(proj_path + '/' + container + '/' + directory + '/' + directory + '.pax.zip.opex', 'a')
            pax_md_hand.write(opex1 + opex2 + opex3 + opex4 + opex5 + opex6 + opex7)
            pax_md_hand.close()
            print('created {}'.format(filename))
            dir_count += 1
    print('Created {} OPEX metdata files for individual assets'.format(dir_count))
    project_log_hand.close()

def stage_pax_content():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    project_log_hand.close()
    pax_count = 0
    rep_count = 0
    for directory in os.listdir(path = proj_path + '/' + container):
        print(directory)
        os.mkdir(proj_path + '/' + container + '/' + directory + '/pax_stage')
        pax_count += 1
        shutil.move(proj_path + '/' + container + '/' + directory + '/Representation_Access', proj_path + '/' + container + '/' + directory + '/pax_stage')
        shutil.move(proj_path + '/' + container + '/' + directory + '/Representation_Preservation', proj_path + '/' + container + '/' + directory + '/pax_stage')
        rep_count += 2
    print('Created {} pax_stage subdirectories and staged {} representation subdirectories'.format(pax_count, rep_count))

def create_pax():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    project_log_hand.close()
    dir_count = 0
    for directory in os.listdir(path = proj_path + '/' + container):
        zip_dir = pathlib.Path(proj_path + '/' + container + '/' + directory + '/pax_stage/')
        pax_obj = ZipFile(proj_path + '/' + container + '/' + directory + '/' + directory + '.zip', 'w')
        for file_path in zip_dir.rglob("*"):
            pax_obj.write(file_path, arcname = file_path.relative_to(zip_dir))
        pax_obj.close()
        os.rename(proj_path + '/' + container + '/' + directory + '/' + directory + '.zip', proj_path + '/' + container + '/' + directory + '/' + directory + '.pax.zip')
        dir_count += 1
        zip_file = dir_count + ': ' + directory + '.pax.zip'
        print('created {}'.format(zip_file))
    print('Created {} PAX archives for ingest'.format(dir_count))

def cleanup_directories():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    file_count = 0
    dir_count = 0
    unexpected = 0
    for directory in os.listdir(path = proj_path + '/' + container):
        for entity in os.listdir(path = proj_path + '/' + container + '/' + directory):
            if entity.endswith('.zip') == True:
                print('PAX: ' + entity)
            elif entity.endswith('.opex') == True:
                print('metadata: ' + entity)
            elif entity.endswith('.xml') == True:
                os.remove(proj_path + '/' + container + '/' + directory + '/' + entity)
                file_count += 1
                print('removed metadata file')
            elif os.path.isdir(proj_path + '/' + container + '/' + directory + '/' + entity) == True:
                shutil.rmtree(proj_path + '/' + container + '/' + directory + '/' + entity)
                dir_count += 1
                print('removed pax_stage directory')
            else:
                print('***UNEXPECTED ENTITY: ' + entity)
                unexpected += 1
    print('Deleted {} metadata files and {} Representation_Preservation and Representation_Access folders'.format(file_count, dir_count))
    print('Found {} unexpected entities'.format(unexpected))
    project_log_hand.close()

def ao_opex_metadata():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    project_log_hand.close()
    file_count = 0
    id_hand = open(proj_path + '/' + 'mcgraw_aonum_islid.txt', 'r')
    id_list = id_hand.readlines()
    id_hand.close()
    for directory in os.listdir(path = proj_path + '/' + container):
        opex_hand = open(proj_path + '/' + container + '/' + directory + '/' + directory + '.pax.zip.opex', 'r')
        opex_str = opex_hand.read()
        opex_hand.close()
        ao_num = ''
        for line in id_list:
            ids = line.split('|')
            aonum = ids[0]
            aonum = aonum.strip()
            isnum = ids[1]
            isnum = isnum.strip()
            if opex_str.find(isnum) != -1:
                ao_num = aonum
                print('found a match for {} and {}'.format(aonum, isnum))
        opex = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Properties><opex:Title>' + ao_num + '</opex:Title><opex:Identifiers><opex:Identifier type="code">' + ao_num + '</opex:Identifier></opex:Identifiers></opex:Properties><opex:DescriptiveMetadata><LegacyXIP xmlns="http://preservica.com/LegacyXIP"><Virtual>false</Virtual></LegacyXIP></opex:DescriptiveMetadata></opex:OPEXMetadata>'
        ao_md_hand = open(proj_path + '/' + container + '/' + directory + '/' + ao_num + '.opex', 'a')
        ao_md_hand.write(opex)
        ao_md_hand.close()
        os.rename(proj_path + '/' + container + '/' + directory, proj_path + '/' + container + '/' + ao_num)
        file_count += 1
    print('Created {} archival object metadata files'.format(file_count))

def write_opex_container_md():
    project_log_hand = open(proj_path + '/' + 'project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    opex1 = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Transfer><opex:Manifest><opex:Folders><opex:Folder>'
    opex2 = container
    opex3 = '</opex:Folder></opex:Folders></opex:Manifest></opex:Transfer></opex:OPEXMetadata>'
    container_opex_hand = open(proj_path + '/' + container + '/' + container + '.opex', 'w')
    container_opex_hand.write(opex1 + opex2 + opex3)
    print('Created OPEX metadata file for {} directory'.format(container))
    project_log_hand.close()
    container_opex_hand.close()

# create_container()
# folder_ds_files()
# create_bags_dir()
# extract_bags()
# validate_bags()
# create_id_ss()
# representation_preservation()
# process_bags()
# representation_access()
# access_id_path()
# merge_access_preservation()
# cleanup_bags()
# pax_metadata()
# stage_pax_content()
# create_pax()
# cleanup_directories()
# ao_opex_metadata()
# write_opex_container_md()
