import os
import re
from base64 import b64decode
from typing import Union

import pandas as pd
import requests
import xmltodict
from dotenv import load_dotenv
from lxml import etree, objectify


class API:
    def __init__(self, username: str = None, password: str = None, host_url: str = None):
        self.username = username
        self.password = password
        self.host_url = host_url
        self.init_args(
            init_args=locals(),
            required_arg_keys=["username", "password", "host_url"],
        )

    def init_args(self, init_args: dict, required_arg_keys: list, env_var_key_prefix: list):
        load_dotenv()
        undefined_arg_keys = []
        undefined_env_var_keys = []
        for arg_key, arg_value in init_args.items():
            if arg_key == "self":
                pass
            elif arg_value is not None:
                setattr(self, arg_key, arg_value)
            elif os.getenv(f"{env_var_key_prefix}{arg_key}", None) is not None:
                setattr(self, arg_key, os.getenv(f"{env_var_key_prefix}{arg_key}"))
            elif arg_key in required_arg_keys:
                undefined_arg_keys.append(arg_key)
                undefined_env_var_keys.append(f"{env_var_key_prefix}{arg_key}")

        if len(undefined_arg_keys) > 0:
            raise Exception(f"Required params are not defined as {undefined_arg_keys} in __init__ args, " + f"or as {undefined_env_var_keys} in environment variables.")

    def get_response(self, api_endpoint: str, query_params: dict = None) -> Union[list, bool]:
        # prepare url and query params
        url = f"{self.host_url}{api_endpoint}"
        if query_params is None:
            query_params = {}
        if "username" not in query_params:
            query_params["username"] = self.username
        if "password" not in query_params:
            query_params["password"] = self.password

        # get response and check status
        response = requests.get(url, query_params, timeout=10)
        response.raise_for_status()

        if response.headers["Content-Type"] == "text/xml; charset=utf-8":
            # prepare xml
            xml_root = objectify.fromstring(
                response.content,
                parser=etree.XMLParser(encoding="utf-8"),
            )
            xml_tag = re.sub(r"[\{].*?[\}]", "", xml_root.tag)
            xml_text = xml_root.text

            # check for known errors
            if xml_text is None:
                raise Exception("No response content, which can be because of incorrect credentials/parameters," + " e.g. filename does not exist")
            if isinstance(xml_text, str):
                known_error_messages = [
                    "Account is locked",
                    "Invalid username or password",
                    "Error Message: Value cannot be null.\nParameter name: clientId",
                ]
                for error_message in known_error_messages:
                    if error_message in xml_text:
                        raise Exception(error_message)

            # convert xml depending on root tag and structure
            if xml_tag == "base64Binary" and isinstance(xml_text, str):
                response_content = b64decode(xml_text)
            elif xml_tag == "string" and isinstance(xml_text, str):
                response_content = xmltodict.parse('<root>' + xml_text.replace("&", "&amp;")+ '</root')
            else:
                response_content = xml_text
        else:
            response_content = response.content
        return response_content

    def get_ids(self, profession_id: str) -> pd.DataFrame:
        api_endpoint = "GetPayerIDs"
        query_params = {"professionID": profession_id}
        response = self.get_response(api_endpoint, query_params)
        df = pd.DataFrame(response["Payers"]["Payer"])
        df = df.rename(columns={"Name": "name", "PayerID": "payer_id"})
        return df

    def get_unviewed_files(self) -> pd.DataFrame:
        api_endpoint = "GetUnviewedFiles"
        response = self.get_response(api_endpoint)
        df = pd.DataFrame(response["fileList"])
        df = df.rename(columns={"file": "filename"})
        return df

    def get_unviewed_eram_files(self) -> pd.DataFrame:
        api_endpoint = "GetUnviewedERAMFiles"
        response = self.get_response(api_endpoint)
        df = pd.DataFrame(response["fileList"])
        df = df.rename(columns={"file": "filename"})
        return df

    def get_file(self, filename: str, save_dir: str = None) -> str:
        api_endpoint = "GetFileByName"
        query_params = {"filename": filename}
        response = self.get_response(api_endpoint, query_params)
        if save_dir:
            file_path = os.path.join(save_dir, filename)
            with open(file_path, "wb") as f:
                f.write(response)
            return file_path
        return response