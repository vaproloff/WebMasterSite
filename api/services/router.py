from fastapi import APIRouter, Request, Depends

from api.auth.auth_config import RoleChecker
from services.load_all_queries import get_all_data as get_all_data_queries

from services.load_all_urls import get_all_data as get_all_data_urls

from services.load_all_history import main as all_history_main

from services.load_query_url_merge import main as merge_main

router = APIRouter()


@router.get('/load-queries-script')
async def load_queries_script(
        request: Request,
        required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
) -> dict:
    try:
        request_session = request.session
        await get_all_data_queries(request_session)
    except Exception as e:
        print(e)
    return {"status": 200}


@router.get('/load-urls-script')
async def load_urls_script(
        request: Request,
        required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
) -> dict:
    try:
        request_session = request.session
        await get_all_data_urls(request_session)
    except Exception as e:
        print(e)
    return {"status": 200}


@router.get('/load-history-script')
async def load_history_script(
        request: Request,
        required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
) -> dict:
    try:
        request_session = request.session
        await all_history_main(request_session)
    except Exception as e:
        print(e)
    return {"status": 200}


@router.get('/load-merge-script')
async def load_merge_script(
        request: Request,
        required: bool = Depends(RoleChecker(required_permissions={"Administrator", "Superuser"}))
) -> dict:
    try:
        request_session = request.session
        await merge_main(request_session)
    except Exception as e:
        print(e)
    return {"status": 200}
