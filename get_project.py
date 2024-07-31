# Import dependencies 
from tqdm import tqdm
from encord import EncordUserClient

from encord.workflow import(
  AnnotationStage, 
  ReviewStage,
  ConsensusAnnotationStage, 
  ConsensusReviewStage,
  FinalStage
)

# Authenticate using the path to your private key
user_client = EncordUserClient.create_with_ssh_private_key(ssh_private_key_path="/Users/encord-anu/Desktop/key/key2.txt")

# Get project details using the "<project_hash>" as an identifier
# project = user_client.get_project("265ff35b-b95e-40bf-a14b-d28570a0026c")
project = user_client.get_project("fbe5a1cc-add0-44e2-afb7-682eb84aee68")

# label_rows = project.list_label_rows_v2()


BATCH_SIZE = 500 # Batch size to split
label_rows = project.list_label_rows_v2()
# data_hases = [lr.data_hash for lr in label_rows]


for l in label_rows:
    print(l.width, l.height)

consensus_stage_annoatate = project.workflow.get_stage(name="Consensus", type_=ConsensusAnnotationStage)
print()
with project.create_bundle() as bundle:
    for annotate in consensus_stage_annoatate.get_tasks(data_hash=data_hases):
            for task in annotate.subtasks:
              print('Moving Consensus Annotate to next stage:', task.data_title )
              task.submit(bundle=bundle)


consensus_stage_review = project.workflow.get_stage(name="Consensus Review", type_=ConsensusReviewStage)
with project.create_bundle() as bundle:
    for task in consensus_stage_review.get_tasks(data_hash=data_hases):
          task.approve(bundle=bundle)
          print('Moving Consensus Review to next stage:', task.data_title )





# label_row_batches = [label_rows[i:i+BATCH_SIZE] for i in range(0, len(label_rows), BATCH_SIZE)] # This code splits the label_rows into batches of size BATCH_SIZE
# for labels_batch in label_row_batches:



#   review_stage = project.workflow.get_stage(name="Consensus 1 Review", type_=ConsensusReviewStage)
#   #bundle_assign = project.create_bundle()
#   #bundle_approve = project.create_bundle()
#   with project.create_bundle() as bundle:
#     for task in review_stage.get_tasks(data_hash=labels_batch):
#             # task.assign("anurag@encord.com", bundle=bundle_assign)
#             task.approve(bundle=bundle)

#   #bundle_assign.execute()
  #bundle_approve.execute()




import pandas as pd

def main(data_file, key_file, project_hash):
  df = pd.read_parquet(data_file)
  df["basename"] = df["s3key"].apply(lambda x: Path(x).name)
  grouped = df.groupby("basename")

  user_client = EncordUserClient.create_with_ssh_private_key(
      ssh_private_key_path=key_file
  )
  project = user_client.get_project(project_hash)
  ontology = project.ontology_structure

  label_rows = project.list_label_rows_v2()
  for label_row_batch in tqdm(
      chunk(label_rows, 100),
      total=int(math.ceil(len(label_rows)) / 100),
      position=0,
  ):
      with project.create_bundle() as bundle:
          for label_row in label_row_batch:
              label_row.initialise_labels(overwrite=True, bundle=bundle)
          bundle.execute()

      with project.create_bundle() as bundle:
          for label_row in label_row_batch:
              width, height = label_row.width, label_row.height
              for _, row in grouped.get_group(label_row.data_title).iterrows():
                  if row["severity"] < 4:
                      continue
                  polygon = parse_polygon(row["points_json"])
                  if row["anomaly_type"] in ("transverse_crack", "crack_transversal"):
                      object = ontology.get_child_by_title(
                          title=f"Transverse Crack - {row['severity']}"
                      )
                  elif row["anomaly_type"] in (
                      "longitudinal_crack",
                      "crack_longitudinal",
                  ):
                      object = ontology.get_child_by_title(
                          title=f"Longitudinal Crack - {row['severity']}"
                      )
                  instance = object.create_instance()
                  instance.set_for_frames(
                      coordinates=PolygonCoordinates(
                          [
                              PointCoordinate(x / width, y / height)
                              for x, y in polygon.exterior.coords
                          ]
                      ),
                      frames=0,
                      manual_annotation=True,
                      overwrite=True,
                  )
                  label_row.add_object_instance(instance)
              label_row.save(bundle=bundle)
          bundle.execute()