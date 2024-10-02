from enum import Enum

date_format_2 = "%Y-%m-%d"

date_format_out = "%d.%m.%Y"

date_format = "%Y-%m-%d"

query_value = 3000


class ACCESS(Enum):
    QUERIES_FULL = "queries_full"
    QUERIES_VIEW = "queries_view"
    QUERIES_FILTER = "queries_filter"
    QUERIES_EXPORT = "queries_export"
    QUERIES_UPDATE = "queries_update"
    QUERIES_SUM = "queries_sum"

    URL_FULL = "url_full"
    URL_VIEW = "url_view"
    URL_FILTER = "url_filter"
    URL_EXPORT = "url_export"
    URL_UPDATE = "url_update"
    URL_SUM = "url_sum"

    HISTORY_FULL = "history_full"
    HISTORY_VIEW = "history_view"
    HISTORY_EXPORT = "history_export"

    URL_QUERY_MERGE_FULL = "url_query_merge_full"
    URL_QUERY_MERGE_VIEW = "url_query_merge_view"
    URL_QUERY_MERGE_RUN = "url_query_merge_run"

    LIST_PANEL_FULL = "list_panel_full"
    LIST_PANEL_VIEW = "list_panel_view"
    LIST_PANEL_CREATE = "list_panel_create"
    LIST_PANEL_EDIT = "list_panel_edit"
    LIST_PANEL_SHARE = "list_panel_share"

    COMMAND_PANEL_FULL = "command_panel_full"
    COMMAND_PANEL_OWN = "command_panel_own"

    LIVE_SEARCH_FULL = "live_search_full"
    LIVE_SEARCH_USE = "live_search_use"

    USER_FULL = "user_full"
    GROUP_FULL = "group_full"

    def __str__(self):
        return self.value
