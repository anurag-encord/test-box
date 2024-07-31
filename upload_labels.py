import math
from argparse import ArgumentParser
from itertools import islice
from pathlib import Path

import numpy as np
import pandas as pd
from encord import EncordUserClient
from encord.objects.coordinates import PointCoordinate, PolygonCoordinates
from shapely import Polygon, box
from tqdm import tqdm


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def parse_polygon(points_json):
    """
    Parse a polygon and return bounding rectangle as a str
    This str can be parsed into a list using ast.literal_eval
    """
    if (
        not isinstance(points_json, list)
        and not isinstance(points_json, np.ndarray)
        and pd.isna(points_json)
    ):
        return None

    if isinstance(points_json[0], list):
        points_json = points_json[0]

    # Convert dict of points to list of tuples
    points_json = [(point["x"], point["y"]) for point in points_json]

    # If the polygon only has 2 points assume xyxy format and convert to polygon
    if len(points_json) == 2:
        x1, y1 = points_json[0]
        x2, y2 = points_json[1]
        polygon = box(min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2))
    else:
        polygon = Polygon(points_json)
        # Attempt to clean the polygon
        if not polygon.is_valid:
            polygon = polygon.buffer(0)

    return polygon


def main(data_file, key_file, project_hash):
    # df = pd.read_parquet(data_file)
    # df["basename"] = df["s3key"].apply(lambda x: Path(x).name)
    # grouped = df.groupby("basename")

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
                label_row.initialise_labels(overwrite=True,bundle=bundle)
            bundle.execute()

        with project.create_bundle() as bundle:
            for label_row in tqdm(label_row_batch, position=1):
                # for _, row in grouped.get_group(label_row.data_title).iterrows():
                for row in range(len())
                    # if row['severity'] < 4:
                    #     continue
                    # polygon = parse_polygon(row["points_json"])
                    # print(polygon)
                    # if row["anomaly_type"] in ("transverse_crack", "crack_transversal"):
                    #     object = ontology.get_child_by_title(
                    #         title=f"Transverse Crack - {row['severity']}"
                    #     )
                    # elif row["anomaly_type"] in (
                    #     "longitudinal_crack",
                    #     "crack_longitudinal",
                    # ):
                    object = ontology.get_child_by_title(
                            title=f"Longitudinal Crack - {row['severity']}"
                        )
                    instance = object.create_instance()
                    instance.set_for_frames(
                        coordinates=PolygonCoordinates(
                            [PointCoordinate(x, y) for x, y in [(1,2), (2,3), (4,6)]]
                        ),
                        frames=0,
                        manual_annotation=True,
                    )
                    label_row.add_object_instance(instance)
                label_row.save(bundle=bundle)
            bundle.execute()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("data_file")
    parser.add_argument("/Users/encord-anu/Desktop/key/key2.txt")
    parser.add_argument("265ff35b-b95e-40bf-a14b-d28570a0026c")
    args = parser.parse_args()
    main(args.data_file, args.key_file, args.project_hash)