
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging
from .sdk_qianfan_v2 import QianfanClientV2
from module3_lib import extract_fields

logger = logging.getLogger(__name__)
router = APIRouter()
class ExtractReq(BaseModel):
    text: str
@router.post("/extract")
def extract(req: ExtractReq):
    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is empty")
    try:
        cli = QianfanClientV2.from_env()
        data = cli.extract(text)
        return {"data": data, "source": "ai"}
    except Exception as e:
        logger.warning("AI extract failed, fallback to local: %s", e)
        data = extract_fields(text)
        return {"data": data, "source": "local"}
