"""
Coordinates requests and responses to Neo4j and Nominatim.
The main class defined in this file is `FastOpenDataServerSession`.
"""

import json
import logging
import pprint
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd
import requests
from neo4j import GraphDatabase, Session
from pyogrio import read_dataframe
from read_env import (
    FOD_CBSA_2013_SHAPEFILE_FILE,
    FOD_CONCATENATED_CENSUS_BLOCK_GROUP_2019_SHAPEFILES_FILE,
    FOD_CONCATENATED_CONSOLIDATED_SCHOOL_DISTRICT_SHAPEFILES_FILE,
    FOD_CONCATENATED_TRACT_SHAPEFILES_FILE,
    FOD_DEBUG_LEVEL,
    FOD_NEO4J_PASSWORD,
    FOD_NEO4J_URL,
    FOD_NEO4J_USER,
    FOD_NOMINATIM_URL,
    FOD_SCHOOL_DISTRICT_SHAPEFILE_FILE,
)
from shapely import Point

logging.basicConfig(level=getattr(logging, FOD_DEBUG_LEVEL))


class GeographyException(Exception):
    pass


class IncompleteDataException(Exception):
    pass


class NominatimQueryException(Exception):
    pass


@dataclass
class NominatimResponse:
    point: Point
    success: bool


class Geography:
    """
    The Geography class represents a geographic entity.

    Attributes:
    * geoid (str): The unique identifier for the geography.

    Methods:
    * init(self, geoid: str = ""): Initialize a new Geography object.
    * repr(self): Return a string representation of the Geography object.
    * get_data(self, neo4j_session) -> dict: Get the data for the Geography object from a Neo4j database.
    """

    def __init__(self, geoid: str = ""):
        self.geoid = geoid

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.geoid}"

    def get_data(self, neo4j_session) -> dict:
        """
        Get the data from Neo4j.

        Args:
            * neo4j_session: A Neo4jSession object.

        Returns:
            A dict containing the data from Neo4j.
        """
        # Return empty dict if we don't have a geoid
        if self.geoid is None:
            return {}
        attributes = ", ".join(
            [f"n.{attribute} AS {attribute}" for attribute in self.attributes]
        )
        cypher_query = (
            f"MATCH (n:{self.__class__.__name__}) "
            f'WHERE n.id = "{self.geoid}" RETURN {attributes};'
        )
        logging.info("Querying Neo4j...")
        result = neo4j_session.run(cypher_query)
        logging.info("Done with query.")
        data = result.data()
        if data:
            result = data[0]
        else:
            result = {}
        return result


class CBSA2013(Geography):
    """
    The core based statistical areas as defined in 2013. We have to use
    the 2013 version because that's what the American Housing Survey
    from 2021 uses.
    """

    attributes = [
        "id",
        "homes_owned_or_bought_by_member_of_household_count",
        "homes_rented_count",
        "homes_occupied_without_rent_payment_count",
        "homes_with_solar_panels_count",
    ]


class County(Geography):
    """
    County objects are individuated by five digits: two digits for
    the state FIPS, followed by three for the County FIPS.
    """

    attributes = [
        "all_teeth_lost_percent",
        "arthritis_percent",
        "asthma_percent",
        "binge_drinking_percent",
        "cancer_percent",
        "cervical_cancer_screening_percent",
        "cholesterol_screen_past_year_percent",
        "chronic_kidney_disease_percent",
        "chronic_obstructive_pulmonary_disease_percent",
        "colon_screen_past_year_percent",
        "coronary_heart_disease_percent",
        "county_fips",
        "county_name",
        "depression_percent",
        "diabetes_percent",
        "fair_poor_health_status_percent",
        "high_blood_pressure_percent",
        "high_cholesterol_percent",
        "lack_health_insurance_percent",
        "less_than_seven_hours_sleep_percent",
        "mammography_percent",
        "medium_to_fair_condition_bridges_percent",
        "men_core_preventative_services_percent",
        "mental_health_not_good_percent",
        "no_leisure_physical_activity_percent",
        "non_commercial_civil_public_use_airports_and_seaplane_base",
        "non_commercial_other_aerodromes",
        "bridges_count",
        "business_establishment_count",
        "resident_worker_count",
        "resident_workers_who_commute_to_work_in_other_counties_count",
        "resident_workers_who_commute_within_county_count",
        "resident_workers_who_work_at_home_count",
        "residents_count",
        "workers_from_other_counties_who_commute_to_work_in_the_county_count",
        "obesity_percent",
        "percent_of_resident_workers_who_commute_by_transit",
        "physical_health_not_good_percent",
        "poor_condition_bridges_percent",
        "primary_and_commercial_airports",
        "route_miles_of_freight_railroad",
        "route_miles_of_passenger_railroad_and_rail_transit",
        "routine_checkup_past_year_percent",
        "smoking_percent",
        "state_fips",
        "state_name",
        "stroke_percent",
        "taking_blood_pressure_medication_percent",
        "docks_count",
        "visited_dentist_past_year_percent",
        "women_core_preventative_services_percent",
    ]


class CongressionalDistrict(Geography):
    attributes = [
        "id",
    ]


class PUMA(Geography):
    attributes = [
        "american_citizen_percent",
        "born_abroad_united_states_citizen_parent_percent",
        "born_in_united_states_percent",
        "born_in_united_states_territory_percent",
        "divorced_percent",
        "id",
        "internet_access_by_paying_provider_percent",
        "language_not_english_spoken_at_home_percent",
        "married_percent",
        "married_spouse_not_present_percent",
        "married_spouse_present_percent",
        "never_married_percent",
        "not_united_states_citizen_percent",
        "only_english_spoken_at_home_percent",
        "separated_percent",
        "united_states_citizen_by_naturalization_percent",
        "widowed_percent",
        "arabic_percent",
        "arabic_speak_english_less_than_very_well_percent",
        "arabic_speak_english_very_well_percent",
        "chinese_incl_mandarin_cantonese_percent",
        "chinese_incl_mandarin_cantonese_speak_english_less_than_very_well_percent",
        "chinese_incl_mandarin_cantonese_speak_english_very_well_percent",
        "french_haitian_or_cajun_percent",
        "french_haitian_or_cajun_speak_english_less_than_very_well_percent",
        "french_haitian_or_cajun_speak_english_very_well_percent",
        "geographic_area_name",
        "german_or_other_west_germanic_languages_percent",
        "german_or_other_west_germanic_languages_speak_english_less_than_very_well_percent",
        "german_or_other_west_germanic_languages_speak_english_very_well_percent",
        "korean_percent",
        "korean_speak_english_less_than_very_well_percent",
        "korean_speak_english_very_well_percent",
        "other_and_unspecified_languages_percent",
        "other_and_unspecified_languages_speak_english_less_than_very_well_percent",
        "other_and_unspecified_languages_speak_english_very_well_percent",
        "other_asian_and_pacific_island_languages_percent",
        "other_asian_and_pacific_island_languages_speak_english_less_than_very_well_percent",
        "other_asian_and_pacific_island_languages_speak_english_very_well_percent",
        "other_indo_european_languages_percent",
        "other_indo_european_languages_speak_english_less_than_very_well_percent",
        "other_indo_european_languages_speak_english_very_well_percent",
        "russian_polish_or_other_slavic_languages_percent",
        "russian_polish_or_other_slavic_languages_speak_english_less_than_very_well_percent",
        "russian_polish_or_other_slavic_languages_speak_english_very_well_percent",
        "spanish_percent",
        "spanish_speak_english_less_than_very_well_percent",
        "spanish_speak_english_very_well_percent",
        "speak_only_english_percent",
        "tagalog_incl_filipino_percent",
        "tagalog_incl_filipino_speak_english_less_than_very_well_percent",
        "tagalog_incl_filipino_speak_english_very_well_percent",
        "vietnamese_percent",
        "vietnamese_speak_english_less_than_very_well_percent",
        "vietnamese_speak_english_very_well_percent",
    ]


class CensusTract(Geography):
    attributes = [
        "arabic_percent",
        "arabic_speak_english_less_than_very_well_percent",
        "arabic_speak_english_very_well_percent",
        "chinese_incl_mandarin_cantonese_percent",
        "chinese_incl_mandarin_cantonese_speak_english_less_than_very_well_percent",
        "chinese_incl_mandarin_cantonese_speak_english_very_well_percent",
        "federal_award_count",
        "french_haitian_or_cajun_percent",
        "french_haitian_or_cajun_speak_english_less_than_very_well_percent",
        "french_haitian_or_cajun_speak_english_very_well_percent",
        "geographic_area_name",
        "german_or_other_west_germanic_languages_percent",
        "german_or_other_west_germanic_languages_speak_english_less_than_very_well_percent",
        "german_or_other_west_germanic_languages_speak_english_very_well_percent",
        "id",
        "korean_percent",
        "korean_speak_english_less_than_very_well_percent",
        "korean_speak_english_very_well_percent",
        "other_and_unspecified_languages_percent",
        "other_and_unspecified_languages_speak_english_less_than_very_well_percent",
        "other_and_unspecified_languages_speak_english_very_well_percent",
        "other_asian_and_pacific_island_languages_percent",
        "other_asian_and_pacific_island_languages_speak_english_less_than_very_well_percent",
        "other_asian_and_pacific_island_languages_speak_english_very_well_percent",
        "other_indo_european_languages_percent",
        "other_indo_european_languages_speak_english_less_than_very_well_percent",
        "other_indo_european_languages_speak_english_very_well_percent",
        "russian_polish_or_other_slavic_languages_percent",
        "russian_polish_or_other_slavic_languages_speak_english_less_than_very_well_percent",
        "russian_polish_or_other_slavic_languages_speak_english_very_well_percent",
        "spanish_percent",
        "spanish_speak_english_less_than_very_well_percent",
        "spanish_speak_english_very_well_percent",
        "speak_only_english_percent",
        "tagalog_incl_filipino_percent",
        "tagalog_incl_filipino_speak_english_less_than_very_well_percent",
        "tagalog_incl_filipino_speak_english_very_well_percent",
        "vietnamese_percent",
        "vietnamese_speak_english_less_than_very_well_percent",
        "vietnamese_speak_english_very_well_percent",
    ]

    def in_puma(self, neo4j_session) -> PUMA:
        cypher_query = (
            f"MATCH (puma:PUMA)<-[:In]-(tract:CensusTract) "
            f'WHERE tract.id = "{self.geoid}" '
            f"RETURN puma.id;"
        )
        result = neo4j_session.run(cypher_query)
        result_data = result.data()
        if not result_data:
            raise IncompleteDataException(
                f"Tried to find a PUMA containing the CensusTract {self.geoid}, "
                f"but could not find one."
            )
        puma_geoid = result_data[0]["puma.id"]
        puma = PUMA(geoid=puma_geoid)
        return puma


class State(Geography):
    attributes = [
        "id",
    ]


class CensusBlockGroup2019(Geography):
    attributes = [
        "households_owning_one_automobile_count",
        "households_owning_one_automobile_percent",
        "households_owning_zero_automobiles_count",
        "housing_units_count",
        "id",
        "occupied_housing_units_count",
        "population_working_percent",
        "total_population",
    ]


class SchoolDistrict(Geography):
    attributes = [
        "id",
        "total_expenditure",
        "total_federal_revenue",
        "total_instruction_spending",
        "total_local_revenue",
        "total_revenue",
        "total_state_revenue",
        "total_support_service_spending",
    ]


def query_nominatim(
    headers: Optional[dict] = None,
    free_form_query: Optional[str] = None,
    street: Optional[str] = None,
    county: Optional[str] = None,
    state: Optional[str] = None,
    postalcode: Optional[str] = None,
    address_details: Optional[bool] = True,
) -> dict:
    """
    Queries the local Nominatim geocoding server.

    Args:
        * headers: Optional headers to send with the request.
        * free_form_query: Optional free-form query to search for.
        * street: Optional street address.
        * county: Optional county name.
        * state: Optional state abbreviation.
        * postalcode: Optional postal code.
        * address_details: Whether to include address details in the response.

    Returns:
        A dictionary containing the response from Nominatim.

    Raises:
        * NominatimQueryException: If no query is provided.
        * NominatimQueryException: If both a free-form query and a detailed address are provided.
    """
    logging.info("Querying nominatim...")

    headers = headers or {}
    has_detailed_address = street or county or state or postalcode
    if not has_detailed_address and not free_form_query:
        raise NominatimQueryException("Must include query to Nominatim")
    elif has_detailed_address and free_form_query:
        raise NominatimQueryException()(
            "Can only use free form query or detailed address, but not both."
        )
    elif free_form_query:
        nominatim_query = {"q": free_form_query}
    else:  # Detailed address has been provided
        nominatim_query = {
            "street": street,
            "county": county,
            "state": state,
            "postalcode": postalcode,
            "addressdetails": int(address_details),
        }
    response = requests.post(FOD_NOMINATIM_URL, headers=headers, params=nominatim_query)
    nominatim_response = response.json()
    logging.info("Reply received from Nominatim...")
    return nominatim_response


def get_latitude_longitude(
    headers: Optional[dict] = None,
    free_form_query: Optional[str] = None,
    street: Optional[str] = None,
    county: Optional[str] = None,
    state: Optional[str] = None,
    postalcode: Optional[str] = None,
    address_details: Optional[bool] = True,
) -> NominatimResponse:
    """
    Gets the latitude and longitude for the specified address.

    Args:
        * headers: HTTP headers to use with the request. Defaults to None.
        * free_form_query: A free-form query to use with the request. Defaults to None.
        * street: The street address. Defaults to None.
        * county: The county name. Defaults to None.
        * state: The state abbreviation. Defaults to None.
        * postalcode: The postal code. Defaults to None.
        * address_details: Whether to include address details in the response. Defaults to True.

    Returns:
        * A NominatimResponse object containing the latitude and longitude.
    """
    nominatim_response = query_nominatim(
        headers=headers,
        free_form_query=free_form_query,
        street=street,
        county=county,
        state=state,
        postalcode=postalcode,
        address_details=address_details,
    )

    if not nominatim_response:
        response = NominatimResponse(point=None, success=False)
        return response

    latitude = nominatim_response[0].get("lat", None)
    longitude = nominatim_response[0].get("lon", None)

    if not (latitude and longitude):
        response = NominatimResponse(point=None, success=False)
    else:
        point = Point(longitude, latitude)
        response = NominatimResponse(point=point, success=True)
    return response


def get_neo4j_session() -> Session:
    """
    Create a `Session` object for Neo4j.
    """
    logging.info("Getting Neo4j session...")
    with GraphDatabase.driver(
        FOD_NEO4J_URL,
        auth=(
            FOD_NEO4J_USER,
            FOD_NEO4J_PASSWORD,
        ),
    ) as driver:
        session = driver.session()
        return session


@dataclass
class GeographyResponse:
    cbsa_2013: CBSA2013 = None
    census_block_group_2019: CensusBlockGroup2019 = None
    congressional_district: CongressionalDistrict = None
    county: County = None
    puma: PUMA = None
    school_district: SchoolDistrict = None
    state: State = None
    tract: CensusTract = None
    success: bool = False


class FastOpenDataServerSession:
    """
    A class for interacting with the FastOpenData (FOD) server.

    The FOD server provides access to a variety of open data sets, including census data,
    school district data, and CBSA data.

    This class provides methods for getting data from the FOD server, including:

    * `get_data_from_address()`: Gets data from an address.
    * `get_geographies_from_point()`: Gets the state, county, census tract, CBSA,
      school district, and congressional district geographies for the specified point.
    """

    def __init__(
        self,
    ):
        """Initialize a new FastOpenDataServerSession object."""
        logging.info("Initializing FastOpenData session...")
        logging.info("Reading concatenated census block group 2019 shapefile...")
        self.census_block_group_2019_dataframe = read_dataframe(
            FOD_CONCATENATED_CENSUS_BLOCK_GROUP_2019_SHAPEFILES_FILE
        )

        logging.info("Reading consolidated school district shapefile...")
        self.consolidated_school_district_dataframe = read_dataframe(
            FOD_CONCATENATED_CONSOLIDATED_SCHOOL_DISTRICT_SHAPEFILES_FILE
        )

        logging.info("Reading school district shapefile...")
        self.school_district_dataframe = read_dataframe(
            FOD_SCHOOL_DISTRICT_SHAPEFILE_FILE
        )

        logging.info("Reading concatenated tract shapefile...")
        self.census_tract_dataframe = read_dataframe(
            FOD_CONCATENATED_TRACT_SHAPEFILES_FILE
        )
        self.census_tract_dataframe.sindex

        logging.info("Reading 2013 CBSA shapefile...")
        self.cbsa_2013_dataframe = read_dataframe(FOD_CBSA_2013_SHAPEFILE_FILE)
        logging.info("Done.")

        logging.info("Getting Neo4j session...")
        self.neo4j_session = get_neo4j_session()
        logging.info("Done.")

    def get_data_from_address(
        self,
        headers: Optional[dict] = None,
        free_form_query: Optional[str] = None,
        street: Optional[str] = None,
        county: Optional[str] = None,
        state: Optional[str] = None,
        postalcode: Optional[str] = None,
        neo4j_session=None,
        census_tract_dataframe=None,
    ):
        """
        Gets data from an address.

        Args:
            free_form_query: A free-form query that can be used to find the address.

        Returns:
            A dict containing the following keys:
                * `state`: The state name.
                * `county`: The county name.
                * `tract`: The census tract.
                * `puma`: The Public Use Microdata Area (PUMA).
                * `school_district`: The school district name.

        Raises:
            `ValueError`: If the free_form_query is not a valid address.
        """

        nominatim_response = get_latitude_longitude(
            free_form_query=free_form_query,
            street=street,
            county=county,
            postalcode=postalcode,
        )
        logging.info(f"Got point: {nominatim_response.point}")
        if not nominatim_response.success:
            return None
        geographies = self.get_geographies_from_point(
            nominatim_response.point,
        )
        if not geographies.success:
            return None

        logging.info(
            f"Got state, county, tract, and school district: "
            f"{geographies.state} :: "
            f"{geographies.county} :: "
            f"{geographies.tract} :: "
            f"{geographies.school_district}"
        )

        state_data = geographies.state.get_data(self.neo4j_session)
        county_data = geographies.county.get_data(self.neo4j_session)
        tract_data = geographies.tract.get_data(self.neo4j_session)
        school_district_data = geographies.school_district.get_data(self.neo4j_session)
        puma_data = geographies.puma.get_data(self.neo4j_session)
        congressional_district = geographies.congressional_district.get_data(
            self.neo4j_session
        )
        cbsa_2013_data = geographies.cbsa_2013.get_data(self.neo4j_session)
        census_block_group_2019_data = geographies.census_block_group_2019.get_data(
            self.neo4j_session
        )

        data = {
            "cbsa_2013": cbsa_2013_data,
            "census_block_group_2019": census_block_group_2019_data,
            "county": county_data,
            "puma": puma_data,
            "school_district": school_district_data,
            "state": state_data,
            "tract": tract_data,
        }

        return data

    def get_geographies_from_point(
        self,
        point: Point,
        get_puma: bool = True,
    ) -> GeographyResponse:
        """
        Gets the state, county, census tract, CBSA, school district, and congressional district
        geographies for the specified point.

        Args:
            point: A point.

        Returns:
            A GeographyResponse object containing the state, county, census tract, CBSA,
            school district, and congressional district geographies.
        """
        matching_series_index = self.census_tract_dataframe.sindex.query(
            point, predicate="within"
        )
        matching_series = self.census_tract_dataframe.iloc[matching_series_index]
        matching_series = self.census_tract_dataframe.iloc[matching_series_index]
        matching_series = self.census_tract_dataframe[
            self.census_tract_dataframe["geometry"].contains(point)
        ][["STATEFP", "COUNTYFP", "TRACTCE"]]

        if matching_series.empty:
            return GeographyResponse(success=False)
        state_code = matching_series["STATEFP"].iloc[0]
        county_code = matching_series["COUNTYFP"].iloc[0]
        tract_code = matching_series["TRACTCE"].iloc[0]

        congressional_district_code = None  # TODO

        matching_series = self.school_district_dataframe[
            self.school_district_dataframe["geometry"].contains(point)
        ][["GEOID"]]
        school_district_code = (
            matching_series["GEOID"].iloc[0] if not matching_series.empty else None
        )
        # school_district_code = matching_series["GEOID"].iloc[0]
        matching_series = self.cbsa_2013_dataframe[
            self.cbsa_2013_dataframe["geometry"].contains(point)
        ][["GEOID"]]
        if matching_series.empty:
            cbsa_2013_code = None
        else:
            cbsa_2013_code = matching_series["GEOID"].iloc[0]
            # cbsa_2013_code = state_code + cbsa_2013_code

        matching_series = self.census_block_group_2019_dataframe[
            self.census_block_group_2019_dataframe["geometry"].contains(point)
        ]
        if not matching_series.empty:
            census_block_group_2019_code = "".join(
                [
                    matching_series["STATEFP"].iloc[0],
                    matching_series["COUNTYFP"].iloc[0],
                    matching_series["TRACTCE"].iloc[0],
                    matching_series["BLKGRPCE"].iloc[0],
                ]
            )
        else:
            census_block_group_2019_code = None

        state = State(geoid=state_code)
        county = County(geoid=state_code + county_code)
        tract = CensusTract(geoid=state_code + county_code + tract_code)
        cbsa_2013 = CBSA2013(geoid=cbsa_2013_code)
        if get_puma:
            puma = tract.in_puma(self.neo4j_session)
        else:
            puma = None
        school_district = SchoolDistrict(geoid=school_district_code)
        congressional_district = CongressionalDistrict(
            geoid=congressional_district_code
        )
        census_block_group_2019 = CensusBlockGroup2019(
            geoid=census_block_group_2019_code
        )
        return GeographyResponse(
            state=state,
            county=county,
            puma=puma,
            tract=tract,
            school_district=school_district,
            congressional_district=congressional_district,
            cbsa_2013=cbsa_2013,
            census_block_group_2019=census_block_group_2019,
            success=True,
        )


if __name__ == "__main__":
    fod = FastOpenDataServerSession()
    data = fod.get_data_from_address(
        # free_form_query="1984 Lower Hawthorne Trail",
        free_form_query="305 Brookhaven",
    )
    data_dump = json.dumps(data, indent=4)
    print(data_dump)
