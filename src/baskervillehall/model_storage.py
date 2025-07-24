from baskervillehall.baskervillehall_isolation_forest import ModelType
from baskervillehall.model_loader import ModelLoader


class ModelStorage(object):

    def __init__(self,
                 s3_connection,
                 s3_path,
                 reload_in_minutes=10,
                 logger=None):
        self.logger = logger

        self.model_storage_human = ModelLoader(
            s3_connection,
            s3_path,
            model_type=ModelType.HUMAN,
            reload_in_minutes=reload_in_minutes,
            logger=self.logger)
        self.model_storage_human.start()

        self.model_storage_bot = ModelLoader(
            s3_connection,
            s3_path,
            model_type=ModelType.BOT,
            reload_in_minutes=reload_in_minutes,
            logger=self.logger)
        self.model_storage_bot.start()

        self.model_storage_generic = ModelLoader(
            s3_connection,
            s3_path,
            model_type=ModelType.GENERIC,
            reload_in_minutes=reload_in_minutes,
            logger=self.logger)
        self.model_storage_generic.start()

    def get_model(self, host, model_type):
        model = None
        if model_type == ModelType.HUMAN:
            model = self.model_storage_human.get_model(host)
        elif model_type == ModelType.BOT:
            model = self.model_storage_bot.get_model(host)

        if model is None or model == ModelType.GENERIC:
            model = self.model_storage_generic.get_model(host)

        return model

