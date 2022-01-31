import os
import shutil
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from bdbag import bdbag_api
from bagit import BagValidationError
from bdbag.bdbagit import BaggingInterruptedError
from pyrsistent import thaw
from zipfile import ZipFile

#------------------------------------------------------------------------------------------------------------------------------------------------------
# WORKFLOW FOR PREPARING ASSETS FOR PRESERVICA INGEST
# Assumes that access assets are coming from Islandora
# Assumes that preservation assets are coming from Digital Scholarship
# Assumes that metadata is coming from Islandora
#------------------------------------------------------------------------------------------------------------------------------------------------------
# 01. Transform single directory of preservation images into separate subdirectories full of images per asset [folder_ds_files()]
# 02. Create container directory which will be dumped into WinSCP for OPEX incremental ingest [create_container()]
# 03. Move directories containing preservation image assets provided by DS into container directory [manually]
# 04. Create 'Representation_Preservation' subdirectories in each asset folder, then move preservation assets into them [representation_preservation()]
# 05. Create bags directory to stage exported bags for processing [create_bags_dir()]
# 06. Move Bags exported from Islandora into bags directory for processing [manually]
# 07. Extract/unzip the bags in the bags directory [extract_bags()]
# 08. Delete the zipped versions of the bags [delete_bags()]
# 09. Validate the unzipped bags to ensure no errors in transfer [validate_bags()]
# 10. Reverts the bags into simple directories and removes unnecessary files in 'data' subdirectory [process_bags()]
# 11. Create 'Representation_Access' subcirectories in each asset folder[representation_access()]
# 12. Generate file containing MODS identifier and relative paths [access_id_path()]
# 13. Loop through dirs in container, move access copies and metadata into relevant folders [merge_access_preservation()]
# 14. Delete the bags_dir folder and the access_ids.txt file once merge is complete [cleanup_bags()]
# 15. Write the OPEX metadata for the entire container structure [write_opex_container_md()]
# 16. Write the OPEX metadata for the individua asset contained in the PAX [pax_metadata()]
# 17. Make a PAX zip archive out of the Representation_Access and Representation_Access [create_pax()]
# 18. Delete the xml files used to create the OPEX metadata and the directories used to create the PAX zip archive [cleanup_directories()]
# 19. Create the OPEX metadata for the archival object folder that syncs with ArchivesSpace, and reanme subdirectories [ao_opex_metadata()]
#------------------------------------------------------------------------------------------------------------------------------------------------------
# Project Log File variables by index
# 0 - date_time
# 1 - container
# 2 - bags_dir
#------------------------------------------------------------------------------------------------------------------------------------------------------

def folder_ds_files():
    #copy folder name provided by DS into *ds_files* variable
    #ds_files is child of preservica_ingest, top level folder
    ds_files = ''
    folder_name = ''
    for file in os.listdir(ds_files):
        file_root = file.split('-')
        file_root = file_root[0]
        folder_count = 0
        file_count = 0
        if  file_root == folder_name:
            shutil.move(ds_files + '/' + file, ds_files + '/' + folder_name + '/' + file)
            file_count += 1
        else:
            folder_name = file_root
            os.mkdir(ds_files + '/' + folder_name)
            folder_count += 1
            shutil.move(ds_files + '/' + file, ds_files + '/' + folder_name + '/' + file)
            file_count += 1
    for folder in os.listdir(ds_files):
        count = 0
        for file in os.listdir(ds_files + '/' + folder):
            count += 1
        if count > 99:
            os.rename(ds_files + '/' + folder, ds_files + '/' + folder + "-001-" + str(count))
        elif count > 9:
            os.rename(ds_files + '/' + folder, ds_files + '/' + folder + "-001-0" + str(count))
        else:
            os.rename(ds_files + '/' + folder, ds_files + '/' + folder + "-001-00" + str(count))
    print('Created and renamed {} subdirectories and moved {} files into them'.format(folder_count, file_count))

def create_container():
    #copy folder name provided by DS into *ds_files* variable
    ds_files = ''
    project_log_hand = open('project_log.txt', 'a')
    now = datetime.now()
    date_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    project_log_hand.write(date_time + '\n')
    container = 'container_' + date_time
    os.rename(ds_files, container)
    project_log_hand.write(container + '\n')
    print('Container directory: {}'.format(container))
    project_log_hand.close()

def representation_preservation():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    folder_count = 0
    file_count = 0
    rep_pres = 'Representation_Preservation'
    for directory in os.listdir(path = container):
        path = container + '/' + directory + '/' + rep_pres
        os.mkdir(path)
        folder_count += 1
        for file in os.listdir(path = container + '/' + directory):
            if file == rep_pres:
                continue
            else:
                file_name = file.split('.')
                file_name = file_name[0]
                os.mkdir(path + '/' + file_name)
                shutil.move(container + '/' + directory + '/' + file, path + '/' + file_name + '/' + file)
            file_count += 1
    print('Created {} Representation_Preservation directories | Moved {} files into created directories'.format(folder_count, file_count))
    project_log_hand.close()

def create_bags_dir():
    project_log_hand = open('project_log.txt', 'a')
    vars = project_log_hand.readlines()
    date_time = vars[0]
    date_time = date_time.strip()
    container = vars[1]
    container = container.strip()
    bags_dir = 'bags_' + date_time
    os.mkdir(container + '/' + bags_dir)
    project_log_hand.write(bags_dir + '\n')
    print('Created bags directory: {}'.format(bags_dir))
    project_log_hand.close()

def extract_bags():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    num_bags = 0
    for file in os.listdir(path = container + '/' + bags_dir):
        bdbag_api.extract_bag(container + '/' + bags_dir + '/' + file, output_path = container + '/' + bags_dir, temp=False)
        num_bags += 1
    print('Extracted {} bags'.format(str(num_bags)))
    project_log_hand.close()

def delete_bags():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    num_bags = 0
    for file in os.listdir(path = container + '/' + bags_dir):
        if file.endswith('.zip'):
            os.remove(container + '/' + bags_dir + '/' + file)
        num_bags += 1
    print('Deleted {} zipped bags'.format(str(num_bags)))
    project_log_hand.close()

def validate_bags():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    error_log_handle = open('validation_error_log.txt', 'a')
    num_bags = 0
    for directory in os.listdir(path = container + '/' + bags_dir):
        num_bags += 1
        try:
            bdbag_api.validate_bag(container + '/' + bags_dir + '/' + directory, fast = False)
        except BagValidationError:
            error_log_handle.write('Bag Validation Error | Directory: ' + directory + '\n')
        except BaggingInterruptedError:
            error_log_handle.write('Bagging Interruped Error | Directory: ' + directory + '\n')
        except RuntimeError:
            error_log_handle.write('Runtime Error | Directory: ' + directory + '\n')
    print('Validated {} bags'.format(str(num_bags)))
    error_log_handle.close()
    project_log_hand.close()

def process_bags():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    num_bags = 0
    error_log_str = ''
    error_log_handle = open('validation_error_log.txt', 'r')
    error_log = error_log_handle.read()
    for line in error_log:
        error_log_str = error_log_str + line
    for directory in os.listdir(path = container + '/' + bags_dir):
        #skips any directories that raised errors during validation
        if error_log_str.find(directory) != -1 :
            continue
        else:
            obj_file_name = ''
            #converts the bags back into normal directories, removing bagit and manifest files
            bdbag_api.revert_bag(container + '/' + bags_dir + '/' + directory)
            #removes unnecessary files generated by Islandora
            unneccesary_files = ['foo.xml', 'foxml.xml', 'JP2.jp2', 'JPG.jpg', 'POLICY.xml', 'RELS-EXT.rdf', 'RELS-INT.rdf', 'TN.jpg', 'HOCR.html', 'OCR.txt', 'MP4.mp4', 'PROXY_MP3.mp3']
            for file in os.listdir(path = container + '/' + bags_dir + '/' + directory):
                if file in unneccesary_files:
                    os.remove(container + '/' + bags_dir + '/' + directory + '/' + file)
                if re.search('^OBJ', file):
                    obj_file_name = file
            #use xml.etree to identify filename from MODS.xml
            tree = ET.parse(container + '/' + bags_dir + '/' + directory + '/MODS.xml')
            identifier = tree.find('{http://www.loc.gov/mods/v3}identifier').text
            #rename the OBJ file to original filename pulled from MODS.xml
            os.rename(container + '/' + bags_dir + '/' + directory + '/' + obj_file_name, container + '/' + bags_dir + '/' + directory + '/' + identifier)
        num_bags += 1
    error_log_handle.close()
    print('Processed {} bags'.format(str(num_bags)))
    project_log_hand.close()

def representation_access():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    folder_count = 0
    rep_acc = 'Representation_Access'
    for directory in os.listdir(path = container):
        if directory.startswith('bags_'):
            continue
        else:
            path = container + '/' + directory + '/' + rep_acc
            os.mkdir(path)
        folder_count += 1
    print('Created {} Representation_Access directories'.format(folder_count))
    project_log_hand.close()

def access_id_path():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    access_id_hand = open('access_ids.txt', 'a')
    access_count = 0
    for directory in os.listdir(path = container + '/' + bags_dir):
        tree = ET.parse(container + '/' + bags_dir + '/' + directory + '/MODS.xml')
        identifier = tree.find('{http://www.loc.gov/mods/v3}identifier').text
        access_id_hand.write(identifier + '|')
        path = container + '/' + bags_dir + '/' + directory
        access_id_hand.write(path + '\n')
        access_count += 1
    print('Logged {} paths and identifiers in access_ids.txt'.format(access_count))
    project_log_hand.close()
    access_id_hand.close()

def merge_access_preservation():
    project_log_hand = open('id_list.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    access_id_hand = open('access_ids.txt', 'r')
    access_id_list = access_id_hand.readlines()
    rep_acc = 'Representation_Access'
    file_count = 0
    for directory in os.listdir(path = container):
        if directory.startswith('bags_'):
            continue
        else:
            for line in access_id_list:
                access_info = line.split('|')
                identifier = access_info[0]
                identifier = identifier.strip()
                path = access_info[1]
                path = path.strip()
                if identifier == directory:
                    for file in os.listdir(path = path):
                        if file.endswith('.xml'):
                            shutil.move(path + '/' + file, container + '/' + directory + '/' + file)
                            file_count += 1
                        else:
                            file_name = file.split('.')
                            file_name = file_name[0]
                            os.mkdir(container + '/' + directory + '/' + rep_acc + '/' +  file_name)
                            shutil.move(path + '/' + file, container + '/' + directory + '/' + rep_acc + '/' + file_name + '/' + file)
                            file_count += 1
    print('Moved {} access and metadata files'.format(file_count))
    project_log_hand.close()
    access_id_hand.close()

def cleanup_bags():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    shutil.rmtree(container + '/' + bags_dir)
    os.remove('access_ids.txt')
    print('Deleted "{}" directory and access_ids.txt'.format(bags_dir))
    project_log_hand.close()

def write_opex_container_md():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    opex1 = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Transfer><opex:Manifest><opex:Folders><opex:Folder>'
    opex2 = container
    opex3 = '</opex:Folder></opex:Folders></opex:Manifest></opex:Transfer></opex:OPEXMetadata>'
    container_opex_hand = open(container + '/' + container + '.opex', 'w')
    container_opex_hand.write(opex1 + opex2 + opex3)
    print('Created OPEX metadata file for {} directory'.format(container))
    project_log_hand.close()
    container_opex_hand.close()

def pax_metadata():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    bags_dir = vars[2]
    bags_dir = bags_dir.strip()
    dir_count = 0
    for directory in os.listdir(path = container):
        opex1 = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Properties><opex:Title>'
        tree = ET.parse(container + '/' + directory + '/DC.xml')
        root = tree.getroot()
        opex2 = tree.find('{http://purl.org/dc/elements/1.1/}title').text
        opex3 = '</opex:Title><opex:Identifiers>'
        id_list = []
        opex4 = ''
        for id in root.iter('{http://purl.org/dc/elements/1.1/}identifier'):
            id_list.append(id.text)
        for id_item in id_list:
            if id_item.startswith('ur'):
                opex4 += '<opex:Identifier type="code">' + id + '</opex:Identifier>'
            else:
                other_identifiers = id_item.split(':')
                label = other_identifiers[0]
                label = label.strip()
                value = other_identifiers[1]
                value = value.strip()
                opex4 += '<opex:Identifier type="' + label + '">' + value + '</opex:Identifier>'
        opex5 = '</opex:Identifiers></opex:Properties><opex:DescriptiveMetadata><LegacyXIP xmlns="http://preservica.com/LegacyXIP"><AccessionRef>catalogue</AccessionRef></LegacyXIP>'
        opex6 = ''
        for file in directory:
            if file.endswith('.xml'):
                temp_file_hand = open(file, 'r')
                lines = temp_file_hand.readlines()
                for line in lines:
                    opex6 += line
                temp_file_hand.close()
        opex7 = '</opex:DescriptiveMetadata></opex:OPEXMetadata>'
        pax_md_hand = open(container + '/' + directory + '.pax.zip.opex', 'a')
        pax_md_hand.write(opex1 + opex2 + opex3 + opex4 + opex5 + opex6 + opex7)
        pax_md_hand.close()
        dir_count += 1
    print('Created {} OPEX metdata files for individual assets'.format(dir_count))
    project_log_hand.close()

def create_pax():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    dir_count = 0
    rep_list = ['Representation_Preservation', 'Representation_Access']
    for directory in os.listdir(path = container):
        zip_obj = ZipFile(container + '/' + directory + '/' + directory + '.pax.zip', 'a')
        for representation in directory:
            if representation in rep_list:
                zip_obj.write(representation)
        zip_obj.close()
        dir_count += 1
    print('Created {} PAX archives for ingest'.format(dir_count))
    project_log_hand.close()

def cleanup_directories():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    file_count = 0
    dir_count = 0
    for directory in os.listdir(path = container):
        for object in directory:
            if object.endswith('.zip') == True:
                continue
            elif object.endswith('.opex') == True:
                continue
            elif object.endswith('.xml') == True:
                os.remove(container + '/' + directory + '/' + object)
                file_count += 1
            else:
                shutil.rmtree(container + '/' + directory + '/' + object)
                dir_count += 1
    print('Deleted {} metadata files and {} Representation_Preservation and Representation_Access folders'.format(file_count, dir_count))
    project_log_hand.close()

def ao_opex_metadata():
    project_log_hand = open('project_log.txt', 'r')
    vars = project_log_hand.readlines()
    container = vars[1]
    container = container.strip()
    file_count = 0
    for directory in os.listdir(path = container):
        # TODO ao_num variable needs to pull from list of Archival Object Numbers pulled from ASpace
        # For each directory, loop through file, file relevant AO number and store in ao_num variable
        ao_num = ''
        ao_md_hand = open(container + '/' + directory + '/' + directory + '.opex', 'a')
        opex = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><opex:OPEXMetadata xmlns:opex="http://www.openpreservationexchange.org/opex/v1.0"><opex:Properties><opex:Title>' + ao_num + '</opex:Title><opex:Identifiers><opex:Identifier type="code">' + ao_num + '</opex:Identifier></opex:Identifiers></opex:Properties><opex:DescriptiveMetadata><LegacyXIP xmlns="http://preservica.com/LegacyXIP"><Virtual>false</Virtual></LegacyXIP></opex:DescriptiveMetadata></opex:OPEXMetadata>'
        ao_md_hand.write(opex)
        ao_md_hand.close()
        os.rename(container + '/' + directory, container + '/' + ao_num)
        file_count += 1
    print('Created {} archival object metadata files'.format(file_count))
    project_log_hand.close()

# folder_ds_files()
# create_container()
# representation_preservation()
# create_bags_dir()
# extract_bags()
# delete_bags()
# validate_bags()
# process_bags()
# representation_access()
# access_id_path()
# merge_access_preservation()
# cleanup_bags()
# write_opex_container_md()
# pax_metadata()
# create_pax()
# cleanup_directories()
# ao_opex_metadata()