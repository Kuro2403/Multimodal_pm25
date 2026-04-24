import os


def test_harness_smoke() -> None:
    assert os.getenv("AIR_QUALITY_TEST_IN_DOCKER") == "1"


def test_gdal_import_smoke() -> None:
    from osgeo import gdal

    assert isinstance(gdal.__version__, str)
    assert gdal.__version__


def test_eccodes_import_smoke() -> None:
    import eccodes

    assert hasattr(eccodes, "codes_get_api_version")
