from tests.conftest import dfassert
from urllib import urlopen
from zipfile import ZipFile
import os
import pytest
from pyspark.sql import Row, Column
from pyspark.sql.functions import col

from magellan.types import Point


@pytest.fixture
def utah_census_tracts_file(request):
    dir = str(request.config.cache.makedir('utah_census_tracts'))
    path = os.path.join(dir, "utah.zip")
    if not os.path.exists(path):
        print "Downloading utah census data..."
        zipurl = 'https://www2.census.gov/geo/tiger/TIGER2017/TRACT/tl_2017_49_tract.zip'
        zipresp = urlopen(zipurl)
        tempzip = open(path, "wb")
        tempzip.write(zipresp.read())
        tempzip.close()
        zf = ZipFile(path)
        zf.extractall(path=dir)
        zf.close()
    return dir


@pytest.fixture
def raw_utah_census_tracts(spark, utah_census_tracts_file):
    """
    :return: DataFrame[
        point: Point
        polyline: Polyline
        polygon: polygon
        metadata: map[str, str]
        valid: boolean
    ]
    """
    return (spark.read.
            format("magellan").
            load(utah_census_tracts_file))


@pytest.fixture
def utah_census_tracts(raw_utah_census_tracts):
    """
    :return: DataFrame[
        point: Point
        polyline: Polyline
        polygon: Polygon
        metadata: map[str, str]
        valid: boolean
    ]
    """
    return raw_utah_census_tracts.select(
        "polygon",
        col("metadata").getItem('TRACTCE').alias('census_tract'),
    )


@pytest.fixture
def sample_geo_data(spark):
    return spark.createDataFrame([
        Row(point=Point(-112.048416, 41.158510), original_tract=210704, state="utah"),
        Row(point=Point(-111.851600, 40.874520), original_tract=126801, state="utah"),
        Row(point=Point(-111.981401, 41.279666), original_tract=210302, state="utah"),
    ])


@pytest.fixture
def expected_merged_data(spark):
    return spark.createDataFrame([
        Row(point=Point(-112.048416, 41.158510), original_tract=210704, state="utah", census_tract="210704"),
        Row(point=Point(-111.851600, 40.874520), original_tract=126801, state="utah", census_tract="126801"),
        Row(point=Point(-111.981401, 41.279666), original_tract=210302, state="utah", census_tract="210302"),
    ])


def test_point_within_polygon(utah_census_tracts, sample_geo_data, expected_merged_data):
    print "in test 1"
    res = sample_geo_data.join(utah_census_tracts).where(col("point").within("polygon")).drop('polygon')
    dfassert(res, expected_merged_data)


def test_point_within_polygon_indexed(utah_census_tracts, sample_geo_data, expected_merged_data):
    print "in test 2"
    res = sample_geo_data.join(utah_census_tracts.index(1)).where(col("point").within("polygon")).drop('polygon')
    dfassert(res, expected_merged_data)
