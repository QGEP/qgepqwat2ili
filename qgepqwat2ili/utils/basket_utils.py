from .. import config


class BasketUtils:

    def __init__(self, model_classes_interlis, abwasser_session):
        self.model_classes_interlis = model_classes_interlis
        self.abwasser_session = abwasser_session

        self.basket_topic_sia405_administration = None
        self.basket_topic_sia405_abwasser = None
        self.basket_topic_dss = None
        self.basket_topic_kek = None

    def create_basket(self):
        dataset = self.model_classes_interlis.t_ili2db_dataset(
            t_id=1,
            datasetname="teksi-wastewater-export",
        )
        self.abwasser_session.add(dataset)

        self.basket_topic_sia405_administration = self.model_classes_interlis.t_ili2db_basket(
            t_id=2,
            dataset=dataset.t_id,
            topic=config.TOPIC_NAME_SIA405_ADMINISTRATION,
            t_ili_tid=None,
            attachmentkey=dataset.datasetname,
            domains="",
        )
        self.abwasser_session.add(self.basket_topic_sia405_administration)

        self.basket_topic_sia405_abwasser = self.model_classes_interlis.t_ili2db_basket(
            t_id=3,
            dataset=dataset.t_id,
            topic=config.TOPIC_NAME_SIA405_ABWASSER,
            t_ili_tid=None,
            attachmentkey=dataset.datasetname,
            domains="",
        )
        self.abwasser_session.add(self.basket_topic_sia405_abwasser)

        self.basket_topic_dss = self.model_classes_interlis.t_ili2db_basket(
            t_id=4,
            dataset=dataset.t_id,
            topic=config.TOPIC_NAME_DSS,
            t_ili_tid=None,
            attachmentkey=dataset.datasetname,
            domains="",
        )
        self.abwasser_session.add(self.basket_topic_dss)

        self.basket_topic_kek = self.model_classes_interlis.t_ili2db_basket(
            t_id=5,
            dataset=dataset.t_id,
            topic=config.TOPIC_NAME_KEK,
            t_ili_tid=None,
            attachmentkey=dataset.datasetname,
            domains="",
        )
        self.abwasser_session.add(self.basket_topic_kek)
        self.abwasser_session.flush()
