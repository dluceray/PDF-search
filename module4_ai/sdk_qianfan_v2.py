
import os
class QianfanClientV2:
    def __init__(self, api_base: str, ak: str, sk: str, model: str):
        self.api_base = api_base.rstrip("/")
        self.ak = ak
        self.sk = sk
        self.model = model
    @classmethod
    def from_env(cls):
        api_base = os.getenv("QF_API_BASE", "").strip()
        ak = os.getenv("QF_AK", "").strip()
        sk = os.getenv("QF_SK", "").strip()
        model = os.getenv("QF_MODEL", "").strip()
        if not (api_base and ak and sk and model):
            raise RuntimeError("Missing Qianfan v2 env vars (QF_API_BASE/QF_AK/QF_SK/QF_MODEL).")
        return cls(api_base, ak, sk, model)
    def extract(self, text: str):
        return {"project":"","unit":"","sign_method":"","sign_date":""}
