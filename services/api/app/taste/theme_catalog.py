from app.taste.profile_contracts import ThemeCatalogItem

THEME_CATALOG: list[ThemeCatalogItem] = [
    ThemeCatalogItem(
        id="underground_dance",
        label="Underground dance",
        description="Warehouse sets, techno, rave nights, and left-of-mainstream dance floors.",
    ),
    ThemeCatalogItem(
        id="indie_live_music",
        label="Indie live music",
        description="Intimate rooms, touring bands, singer-songwriters, and alt-pop bills.",
    ),
    ThemeCatalogItem(
        id="gallery_nights",
        label="Gallery nights",
        description="Art openings, installations, and neighborhood gallery crawls.",
    ),
    ThemeCatalogItem(
        id="jazz_intimate_shows",
        label="Jazz / intimate shows",
        description="Listening rooms, small ensembles, and musically focused late evenings.",
    ),
    ThemeCatalogItem(
        id="hiphop_rap_shows",
        label="Hip-hop / rap shows",
        description="Rap nights, beat showcases, and high-energy live performances.",
    ),
    ThemeCatalogItem(
        id="comedy_nights",
        label="Comedy",
        description="Stand-up, alt-comedy, and independent comedy rooms.",
    ),
    ThemeCatalogItem(
        id="dive_bar_scene",
        label="Dive bars / local scene",
        description="Neighborhood bars, low-key nights, and local regular spots.",
    ),
    ThemeCatalogItem(
        id="rooftop_lounges",
        label="Rooftop / upscale lounges",
        description="Views, polished spaces, and social nights with a dressed-up feel.",
    ),
    ThemeCatalogItem(
        id="late_night_food",
        label="Late-night food scene",
        description="Food pop-ups, late bites, and destination restaurants worth planning around.",
    ),
    ThemeCatalogItem(
        id="queer_nightlife",
        label="Queer nightlife",
        description="Queer clubs, drag, community-centered parties, and inclusive social nights.",
    ),
    ThemeCatalogItem(
        id="collector_marketplaces",
        label="Collector marketplaces",
        description="Swap communities, niche gear finds, secondary markets, and enthusiast trading culture.",
    ),
    ThemeCatalogItem(
        id="student_intellectual_scene",
        label="Campus / intellectual scene",
        description="Campus culture, talks, readings, and idea-driven social environments.",
    ),
    ThemeCatalogItem(
        id="ambitious_professional_scene",
        label="Ambitious professional scene",
        description="Career-focused energy, networking-forward rooms, and industry-curious social nights.",
    ),
    ThemeCatalogItem(
        id="style_design_shopping",
        label="Style / design shopping",
        description="Menswear, thoughtful retail, design stores, and shopping-led city wandering.",
    ),
    ThemeCatalogItem(
        id="creative_meetups",
        label="Creative meetups",
        description="Maker markets, founder meetups, workshops, and community-built cultural events.",
    ),
]

THEME_CATALOG_BY_ID = {theme.id: theme for theme in THEME_CATALOG}
