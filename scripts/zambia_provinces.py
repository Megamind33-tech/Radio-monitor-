"""
Zambia: map Radio Garden place (district/city) names to provinces.
Radio Garden uses city names as place titles — not provinces.
"""

# Radio Garden place title -> province (10 provinces of Zambia)
PLACE_TO_PROVINCE: dict[str, str] = {
    # Lusaka Province
    "Lusaka": "Lusaka",
    "Kapiri Mposhi": "Central",
    # Central Province
    "Kabwe": "Central",
    # Copperbelt Province
    "Kitwe": "Copperbelt",
    "Ndola": "Copperbelt",
    "Luanshya": "Copperbelt",
    "Chingola": "Copperbelt",
    "Mufulira": "Copperbelt",
    # Northern Province
    "Kasama": "Northern",
    "Mbala": "Northern",
    # Muchinga Province
    "Chinsali": "Muchinga",
    "Isoka": "Muchinga",
    "Nakonde": "Muchinga",
    # Eastern Province
    "Chipata": "Eastern",
    "Petauke": "Eastern",
    "Lundazi": "Eastern",
    "Katete": "Eastern",
    # Southern Province
    "Choma": "Southern",
    "Monze": "Southern",
    "Livingstone": "Southern",
    "Kalomo": "Southern",
    # Western Province
    "Mongu": "Western",
    "Sesheke": "Western",
    "Senanga": "Western",
    # North-Western Province
    "Solwezi": "North-Western",
    "Zambezi": "North-Western",
    "Mwinilunga": "North-Western",
    "Ikelenge": "North-Western",
    # Luapula Province
    "Mansa": "Luapula",
    "Samfya": "Luapula",
    # Fallbacks for RG pages we have seen
    "Itezhi tezhi": "Southern",
}


def province_for_place(place: str) -> str:
    p = (place or "").strip()
    if not p:
        return ""
    return PLACE_TO_PROVINCE.get(p, "")


def province_for_tunein_district(district: str) -> str:
    """When district was inferred from TuneIn name, map to province."""
    return province_for_place(district)
