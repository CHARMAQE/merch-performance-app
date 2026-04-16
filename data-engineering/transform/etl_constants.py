TASK_TABLE_MAP = {
    "LOCATION CHECK IN": "task_location_checkin",
    "LOCATION CHECK OUT": "task_location_checkout",
    "CALLCYCLE DEVIATION": "task_callcycle_deviation",
    "QUALITY": "task_quality",
}

TITLE_TABLE_MAP = {
    "OSA": "task_osa_pack_coc_mh",
    "COC": "task_osa_pack_coc_mh",
    "MH": "task_osa_pack_coc_mh",
    "PACK": "task_osa_pack_coc_mh",
    "LOCALISATION": "task_store_conditions",
    "TEMPERATURE": "task_store_conditions",
    "INFILTRATION": "task_store_conditions",
    "MAINTENANCE": "task_store_conditions",
    "PRIMARY SHELF PLACEMENT": "task_primary_shelf_placement",
    "PRIX": "task_price",
    "SECONDARY PLACEMENT": "task_secondary_placement",
    "SOS": "task_sos",
}

LOCATION_TABLES = {"task_location_checkin", "task_location_checkout"}

TABLES_WITH_PRODUCT = {
    "task_osa_pack_coc_mh",
    "task_price",
}

TABLES_WITH_TITLE = {"task_osa_pack_coc_mh"}

TABLES_WITH_TASK = {
    "task_primary_shelf_placement",
    "task_sos",
}