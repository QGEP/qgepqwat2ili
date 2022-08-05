import json

from qgis import processing
from qgis.core import (
    QgsProcessingContext,
    QgsProcessingFeedback,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterFileDestination,
    QgsProcessingParameterVectorLayer,
)

from ....processing_provider.qgep_algorithm import QgepAlgorithm


class ExtractlabelsInterlisAlgorithm(QgepAlgorithm):
    """This runs the native extractlabels algorithm, but attaches obj_id to the results.

    Otherwise, the output is difficult to use, because the FeatureID isn't stable (since it's the primary
    key of the reach and structure views is not an integer)"""

    OUTPUT = "OUTPUT"
    INPUT_RESTRICT_TO_SELECTION = "RESTRICT_TO_SELECTION"
    INPUT_STRUCTURE_VIEW_LAYER = "STRUCTURE_VIEW_LAYER"
    INPUT_REACH_VIEW_LAYER = "REACH_VIEW_LAYER"

    def name(self):
        return "extractlabels_interlis"

    def displayName(self):
        return self.tr("Extract labels for interlis")

    def initAlgorithm(self, config=None):

        self.addParameter(
            QgsProcessingParameterFileDestination(
                self.OUTPUT,
                description=self.tr("Output labels file"),
                fileFilter="geojson (*.geojson)",
                # TODO: remove defaut value
                defaultValue=r"C:\Users\Olivier\Code\QGEP\qgepplugin\qgepplugin\qgepqwat2ili\my_export_labels.geojson",
            )
        )

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.INPUT_RESTRICT_TO_SELECTION,
                description=self.tr("Restrict to selection"),
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_STRUCTURE_VIEW_LAYER,
                description=self.tr("Structure view layer"),
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_REACH_VIEW_LAYER,
                description=self.tr("Reach view layer"),
            )
        )

    def processAlgorithm(self, parameters, context: QgsProcessingContext, feedback: QgsProcessingFeedback):

        labels_file_path = self.parameterAsFileOutput(parameters, self.OUTPUT, context)
        restrict_to_selection = self.parameterAsBoolean(parameters, self.INPUT_RESTRICT_TO_SELECTION, context)
        structure_view_layer = self.parameterAsVectorLayer(parameters, self.INPUT_STRUCTURE_VIEW_LAYER, context)
        reach_view_layer = self.parameterAsVectorLayer(parameters, self.INPUT_REACH_VIEW_LAYER, context)

        if restrict_to_selection:
            extent = structure_view_layer.boundingBoxOfSelected()
            extent.combineExtentWith(reach_view_layer.boundingBoxOfSelected())
        else:
            extent = structure_view_layer.extent()
            extent.combineExtentWith(reach_view_layer.extent())

        # Export the labels file
        feedback.pushInfo(f"saving labels to {labels_file_path}")
        processing.run(
            "native:extractlabels",
            {
                "DPI": 96,
                # TODO: use selection extent
                "EXTENT": extent,
                "INCLUDE_UNPLACED": True,
                "MAP_THEME": None,
                "OUTPUT": labels_file_path,
                # TODO: make scale customizable
                "SCALE": 1000,
            },
            # this is a child algotihm
            is_child_algorithm=True,
            context=context,
            feedback=feedback,
        )

        # Attach the obj_id here
        # We must do that here through QGIS API, as otherwise rowid does not seem preserved...

        with open(labels_file_path, "r") as labels_file_handle:
            labels = json.load(labels_file_handle)

        reach_feats = reach_view_layer.getFeatures()
        structure_feats = structure_view_layer.getFeatures()

        rowid_to_obj_id = {
            "vw_qgep_reach": {f.id(): f.attribute("obj_id") for f in reach_feats},
            "vw_qgep_wastewater_structure": {f.id(): f.attribute("obj_id") for f in structure_feats},
        }

        for label in labels["features"]:
            lyr = label["properties"]["Layer"]
            rowid = label["properties"]["FeatureID"]
            label["properties"]["qgep_obj_id"] = rowid_to_obj_id[lyr][rowid]

        with open(labels_file_path, "w") as labels_file_handle:
            json.dump(labels, labels_file_handle, indent=2)

        feedback.setProgress(100)

        return {}
