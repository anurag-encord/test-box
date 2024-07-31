import json
from tqdm import tqdm

from encord.utilities.coco.datastructure import (
    CategoryID,
    FrameIndex,
    ImageID,
)

from encord import EncordUserClient
from encord.exceptions import OntologyError

bucket_subpath = 'encord/Top_view_tollbooths_Master_DUK_61_May16_1130-2330_1f_3sec_Labeling_Clone/271_Master_DUK_61_May16_1130-2330_1f_3sec_Labeling_Clone/images'

keyfile = './macbook-air-navar-private-key.txt'
user_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path=keyfile)
project = user_client.get_project('d5d10e40-fc33-48b7-9d35-5ec357eabf56')

# Load in COCO labels
with open('./bc-ferries-coco-subset.json', 'r') as f:
    labels_dict = json.load(f)

# Build category id mapping to Encord ontology items
category_id_to_feature_hash = {}
ont_struct = project.ontology_structure
for coco_category in labels_dict['categories']:
    try:
        ont_obj = ont_struct.get_child_by_title(coco_category['name'])
        category_id_to_feature_hash[coco_category['id']] = ont_obj.feature_node_hash
    except OntologyError:
        print(f'Could not match {coco_category["name"]} in the ontology. Import will crash if these are present.')

# Build image id to data hash mapping
image_id_to_frame_index = {}

for img in tqdm(labels_dict['images'], desc='Processing images'):
    fname = img['file_name']
    stem = fname.split('/')[0]
    matches = project.list_label_rows_v2(data_title_eq=f'{bucket_subpath}/{stem}')
    assert len(matches) == 1, f'Problem matching {stem}'
    lr = matches.pop()
    image_id_to_frame_index[img['id']] = FrameIndex(lr.data_hash, img['id'])
    # Implement matching logic
    # matches = project.list_label_rows_v2(data_title_eq=stem)
    # assert len(matches) == 1, f'Problem matching {stem}'
    # lr = matches.pop()
    image_id_to_frame_index[img['id']] = FrameIndex(lr.data_hash, img['id'])

# Run import
project.import_coco_labels(
    labels_dict,
    category_id_to_feature_hash,
    image_id_to_frame_index
)