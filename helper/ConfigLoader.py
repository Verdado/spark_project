import json


class configLoader:
    def read_config_json():
        with open("./config/table.json") as f:  # TODO can be loaded dynamically
            table_config = json.loads(f.read())
        return table_config
